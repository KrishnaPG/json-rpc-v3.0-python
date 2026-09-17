"""Reusable JSON-RPC 3.0 client request, response, and stream correlation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeAlias
from uuid import uuid4

from .constants import DEFAULT_CANCEL_METHOD, JSONRPC_VERSION
from .messages import UNSET, JsonRpcId, JSONRPCRequest

ClientSender = Callable[[dict[str, object]], Awaitable[None]]
IdFactory = Callable[[], JsonRpcId]
ClientCallback = Callable[[dict[str, object]], Awaitable[None]]
StreamTracker: TypeAlias = "asyncio.Future[object] | JsonRpcStreamCall"


class _StreamTerminal:
    """Private queue sentinel for terminal stream frames."""

    __slots__ = ()


STREAM_TERMINAL = _StreamTerminal()


class JsonRpcClient:
    """Small async JSON-RPC client with method and stream lifecycle correlation."""

    __slots__ = ("_id_factory", "_on_ack", "_on_stream", "_pending", "_send", "_streams")

    def __init__(
        self,
        send: ClientSender,
        *,
        id_factory: IdFactory | None = None,
        on_ack: ClientCallback | None = None,
        on_stream: ClientCallback | None = None,
    ) -> None:
        self._send = send
        self._id_factory = id_factory or _default_id
        self._on_ack = on_ack
        self._on_stream = on_stream
        self._pending: dict[str, asyncio.Future[object]] = {}
        self._streams: dict[str, StreamTracker] = {}

    async def request(
        self,
        method: str,
        params: object = UNSET,
        *,
        options: dict[str, object] | None = None,
    ) -> object:
        """Send a request and await its correlated result."""
        msg_id = self._id_factory()
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        stream_requested = _requests_stream(options)
        self._track_future(msg_id, future, stream_requested)
        try:
            await self._send(
                JSONRPCRequest(
                    method,
                    params=params,
                    options=options,
                    msg_id=msg_id,
                ).to_dict()
            )
        except BaseException:
            self._drop_future(msg_id, stream_requested)
            raise
        return await future

    async def stream(
        self,
        method: str,
        params: object = UNSET,
        *,
        options: dict[str, object] | None = None,
        queue_size: int = 0,
    ) -> JsonRpcStreamCall:
        """Send a stream request and return an async iterator for `stream.data`.

        The iterator yields data payload objects by reference; terminal
        `stream.result` is available through `result()`, and `stream.error`
        raises `JsonRpcClientError` from both iteration and `result()`.
        """
        msg_id = self._id_factory()
        stream = JsonRpcStreamCall(self, msg_id, queue_size=queue_size)
        self._streams[str(msg_id)] = stream
        try:
            await self._send(
                JSONRPCRequest(
                    method,
                    params=params,
                    options=_stream_options(options),
                    msg_id=msg_id,
                ).to_dict()
            )
        except BaseException:
            self._streams.pop(str(msg_id), None)
            raise
        return stream

    async def abort_stream(self, stream_id: JsonRpcId) -> None:
        """Send the schema-defined method-less stream abort request."""
        await self._send(
            {
                "jsonrpc": JSONRPC_VERSION,
                "options": {"stream": stream_id, "abort": True},
            }
        )

    async def cancel_stream(
        self,
        stream_id: JsonRpcId,
        *,
        method: str = DEFAULT_CANCEL_METHOD,
        target_param: str = "id",
    ) -> None:
        """Send an application cancel method as a notification with no root result."""
        await self.notify(method, {target_param: stream_id})

    async def notify(
        self,
        method: str,
        params: object = UNSET,
        *,
        options: dict[str, object] | None = None,
    ) -> None:
        """Send a JSON-RPC notification that must not produce a method response."""
        await self._send(JSONRPCRequest(method, params=params, options=options).to_dict())

    async def receive(self, payload: dict[str, object]) -> None:
        """Resolve a pending request from one inbound response payload."""
        if "ack" in payload:
            self._track_ack(payload)
            await self._emit(self._on_ack, payload)
            return
        if "stream" in payload:
            await self._route_stream(payload)
            await self._emit(self._on_stream, payload)
            return
        msg_id = payload.get("id")
        if msg_id is None:
            return
        tracker = self._pending.pop(str(msg_id), None)
        if tracker is None:
            return
        error = payload.get("error")
        if isinstance(error, dict):
            tracker.set_exception(JsonRpcClientError(error))
            return
        tracker.set_result(payload.get("result"))

    async def fail_all(self, error: BaseException) -> None:
        """Fail every pending request and stream, e.g. on transport loss.

        Transports call this from their reader loop when the connection closes
        so in-flight callers observe the transport error instead of hanging.
        """
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        for tracker in self._streams.values():
            await _fail_stream_tracker_error(tracker, error)
        self._streams.clear()

    def _track_future(
        self,
        msg_id: JsonRpcId,
        future: asyncio.Future[object],
        stream_requested: bool,
    ) -> None:
        if stream_requested:
            self._streams[str(msg_id)] = future
            return
        self._pending[str(msg_id)] = future

    def _drop_future(self, msg_id: JsonRpcId, stream_requested: bool) -> None:
        if stream_requested:
            self._streams.pop(str(msg_id), None)
            return
        self._pending.pop(str(msg_id), None)

    def _track_ack(self, payload: dict[str, object]) -> None:
        ack = payload.get("ack")
        if not isinstance(ack, dict):
            return
        original = ack.get("id")
        stream = ack.get("stream")
        if not isinstance(stream, dict):
            return
        next_stream = stream.get("id")
        if next_stream is None:
            return
        tracker = self._streams.pop(str(original), None)
        if tracker is None:
            return
        _set_stream_id(tracker, next_stream)
        self._streams[str(next_stream)] = tracker

    async def _route_stream(self, payload: dict[str, object]) -> None:
        stream = payload.get("stream")
        if not isinstance(stream, dict):
            return
        stream_id = stream.get("id")
        tracker = self._streams.get(str(stream_id))
        if tracker is None:
            return
        terminal = _stream_terminal_kind(payload)
        if terminal is None:
            await _feed_stream_data(tracker, stream)
            return
        self._streams.pop(str(stream_id), None)
        await _settle_stream_tracker(tracker, payload)

    async def _abort_tracked_stream(self, stream_id: JsonRpcId) -> None:
        await self.abort_stream(stream_id)

    async def _cancel_tracked_stream(
        self,
        stream_id: JsonRpcId,
        *,
        method: str,
        target_param: str,
    ) -> None:
        await self.cancel_stream(stream_id, method=method, target_param=target_param)

    async def _emit(self, callback: ClientCallback | None, payload: dict[str, object]) -> None:
        if callback is None:
            return
        await callback(payload)


class JsonRpcStreamCall(AsyncIterator[object]):
    """Async iterator for one client-initiated JSON-RPC stream."""

    __slots__ = ("_client", "_queue", "_result", "_stream_id")

    def __init__(
        self,
        client: JsonRpcClient,
        stream_id: JsonRpcId,
        *,
        queue_size: int,
    ) -> None:
        self._client = client
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_size)
        self._result: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self._stream_id = stream_id

    @property
    def stream_id(self) -> JsonRpcId:
        """Return the current stream id, including ACK remaps."""
        return self._stream_id

    def __aiter__(self) -> JsonRpcStreamCall:
        """Return this stream as its own async iterator."""
        return self

    async def __anext__(self) -> object:
        """Return the next `stream.data` object or stop after the terminal frame."""
        item = await self._queue.get()
        if item is STREAM_TERMINAL:
            self._raise_terminal_error()
            raise StopAsyncIteration
        return item

    async def result(self) -> object:
        """Return the terminal stream result or raise the terminal stream error."""
        return await asyncio.shield(self._result)

    async def abort(self) -> None:
        """Request method-less abort for this stream's current id."""
        await self._client._abort_tracked_stream(self.stream_id)

    async def cancel(
        self,
        *,
        method: str = DEFAULT_CANCEL_METHOD,
        target_param: str = "id",
    ) -> None:
        """Send an application cancel notification for this stream's current id."""
        await self._client._cancel_tracked_stream(
            self.stream_id,
            method=method,
            target_param=target_param,
        )

    async def feed_data(self, data: object) -> None:
        """Queue one `stream.data` payload by reference for iteration."""
        await self._queue.put(data)

    async def finish(self, result: object) -> None:
        """Complete the stream with a terminal `result` payload."""
        self._set_result(result)
        await self._queue.put(STREAM_TERMINAL)

    async def fail(self, error: dict[str, object]) -> None:
        """Complete the stream with a terminal JSON-RPC error payload."""
        self._set_exception(JsonRpcClientError(error))
        await self._queue.put(STREAM_TERMINAL)

    async def fail_with(self, error: BaseException) -> None:
        """Complete the stream with an arbitrary transport error."""
        if not self._result.done():
            self._result.set_exception(error)
        await self._queue.put(STREAM_TERMINAL)

    def remap(self, stream_id: JsonRpcId) -> None:
        """Move the stream to the id announced by an ACK stream extension."""
        self._stream_id = stream_id

    def _raise_terminal_error(self) -> None:
        try:
            self._result.result()
        except JsonRpcClientError as exc:
            raise exc

    def _set_result(self, result: object) -> None:
        if self._result.done():
            return
        self._result.set_result(result)

    def _set_exception(self, exc: JsonRpcClientError) -> None:
        if self._result.done():
            return
        self._result.set_exception(exc)


class JsonRpcClientError(Exception):
    """Client-side wrapper for a JSON-RPC error response."""

    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, object]) -> None:
        super().__init__(str(payload.get("message", "JSON-RPC error")))
        self.payload = payload


def _default_id() -> str:
    return f"rpc-{uuid4()}"


def _requests_stream(options: dict[str, object] | None) -> bool:
    if options is None:
        return False
    return bool(options.get("stream"))


def _stream_options(options: dict[str, object] | None) -> dict[str, object]:
    if options is None:
        return {"stream": True}
    if options.get("stream"):
        return options
    merged = dict(options)
    merged["stream"] = True
    return merged


def _stream_terminal_kind(payload: dict[str, object]) -> str | None:
    terminal_keys = ("result", "error")
    matches = tuple(key for key in terminal_keys if key in payload)
    if not matches:
        return None
    return matches[0]


def _set_stream_id(tracker: StreamTracker, stream_id: JsonRpcId) -> None:
    if isinstance(tracker, JsonRpcStreamCall):
        tracker.remap(stream_id)


async def _feed_stream_data(
    tracker: StreamTracker,
    stream: dict[str, object],
) -> None:
    if "data" not in stream:
        return
    if isinstance(tracker, JsonRpcStreamCall):
        await tracker.feed_data(stream["data"])


async def _settle_stream_tracker(
    tracker: StreamTracker,
    payload: dict[str, object],
) -> None:
    error = payload.get("error")
    if isinstance(error, dict):
        await _fail_stream_tracker(tracker, error)
        return
    await _finish_stream_tracker(tracker, payload.get("result"))


async def _finish_stream_tracker(tracker: StreamTracker, result: object) -> None:
    if isinstance(tracker, JsonRpcStreamCall):
        await tracker.finish(result)
        return
    tracker.set_result(result)


async def _fail_stream_tracker(
    tracker: StreamTracker,
    error: dict[str, object],
) -> None:
    if isinstance(tracker, JsonRpcStreamCall):
        await tracker.fail(error)
        return
    tracker.set_exception(JsonRpcClientError(error))


async def _fail_stream_tracker_error(
    tracker: StreamTracker,
    error: BaseException,
) -> None:
    if isinstance(tracker, JsonRpcStreamCall):
        await tracker.fail_with(error)
        return
    if not tracker.done():
        tracker.set_exception(error)
