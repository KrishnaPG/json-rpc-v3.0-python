"""Reusable per-session JSON-RPC connection runtime."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .constants import DEFAULT_QUEUE_SIZE
from .messages import JSONRPCAck, JSONRPCError, JsonRpcId, JSONRPCResponse, JSONRPCStream

JsonSender = Callable[[dict[str, object]], Awaitable[None]]


class JsonRpcConnection:
    """Task-aware JSON-RPC connection over an arbitrary async send callable."""

    __slots__ = ("_send", "_send_lock", "_send_queue", "_sender_task", "_tasks")

    def __init__(
        self,
        send: JsonSender,
        *,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        self._send = send
        self._send_lock = asyncio.Lock()
        self._send_queue: asyncio.Queue[dict[str, object]] = asyncio.Queue(maxsize=queue_size)
        self._sender_task: asyncio.Task[None] | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def tasks(self) -> dict[str, asyncio.Task[None]]:
        """Return the active task registry for diagnostics and adapters."""
        return self._tasks

    async def send(self, message: dict[str, object]) -> None:
        """Queue one serialized JSON-RPC message for outbound delivery."""
        self._ensure_sender()
        await self._send_queue.put(message)

    async def send_result(self, result: object, msg_id: JsonRpcId) -> None:
        """Send a non-streaming result response."""
        await self.send(JSONRPCResponse(result=result, msg_id=msg_id).to_dict())

    async def send_error(self, error: JSONRPCError, msg_id: JsonRpcId) -> None:
        """Send a non-streaming error response."""
        await self.send(JSONRPCResponse(error=error, msg_id=msg_id).to_dict())

    async def send_method_ack(
        self,
        msg_id: JsonRpcId,
        ack: dict[str, object] | None = None,
    ) -> None:
        """Send an acknowledgement frame for a non-streaming async request."""
        await self.send(JSONRPCResponse(ack=ack or {}, msg_id=msg_id).to_dict())

    async def send_ack(
        self,
        stream_id: JsonRpcId,
        *,
        stream: dict[str, object] | None = None,
    ) -> None:
        """Send an acknowledgement frame for a streaming request."""
        await self.send(JSONRPCResponse(ack=JSONRPCAck(msg_id=stream_id, stream=stream)).to_dict())

    async def send_stream_data(self, stream_id: JsonRpcId, data: object) -> None:
        """Send one stream data frame."""
        await self.send(JSONRPCResponse(stream=JSONRPCStream(stream_id, data)).to_dict())

    async def send_stream_result(self, stream_id: JsonRpcId, result: object) -> None:
        """Send the terminal stream result frame."""
        await self.send(JSONRPCResponse(result=result, stream=JSONRPCStream(stream_id)).to_dict())

    async def send_stream_error(self, stream_id: JsonRpcId, error: JSONRPCError) -> None:
        """Send the terminal stream error frame."""
        await self.send(JSONRPCResponse(error=error, stream=JSONRPCStream(stream_id)).to_dict())

    def track(self, key: JsonRpcId, task: asyncio.Task[None]) -> None:
        """Register a cancellable task under a request id or application alias."""
        self._tasks[str(key)] = task

    def track_current(self, key: JsonRpcId) -> None:
        """Register the current asyncio task under an additional alias."""
        task = asyncio.current_task()
        if task is None:
            return
        self.track(key, task)

    def cancel(self, key: JsonRpcId) -> bool:
        """Cancel a registered task and report whether it existed."""
        task = self._tasks.get(str(key))
        if task is None:
            return False
        task.cancel()
        return True

    async def cancel_all(self) -> None:
        """Cancel every registered task and clear the registry."""
        for task in tuple(self._tasks.values()):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks.values()), return_exceptions=True)
        self._tasks.clear()

    def untrack_task(self, task: asyncio.Task[None]) -> None:
        """Remove every registry key pointing at the given task."""
        keys = tuple(key for key, value in self._tasks.items() if value is task)
        for key in keys:
            self._tasks.pop(key, None)

    async def flush(self) -> None:
        """Wait until queued outbound messages are sent."""
        await self._send_queue.join()

    async def wait_until_idle(self) -> None:
        """Wait until active tasks and queued outbound frames settle."""
        while self._tasks:
            await asyncio.gather(*tuple(self._tasks.values()), return_exceptions=True)
        await self.flush()

    async def close(self) -> None:
        """Flush outbound frames and stop the sender task."""
        await self.flush()
        if self._sender_task is None:
            return
        self._sender_task.cancel()
        await asyncio.gather(self._sender_task, return_exceptions=True)
        self._sender_task = None

    def _ensure_sender(self) -> None:
        if self._sender_task is not None and not self._sender_task.done():
            return
        self._sender_task = asyncio.create_task(self._send_queued())

    async def _send_queued(self) -> None:
        while True:
            message = await self._send_queue.get()
            try:
                async with self._send_lock:
                    await self._send(message)
            finally:
                self._send_queue.task_done()
