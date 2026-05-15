"""Generic JSON-RPC stream writers and event-sink adapters."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Generic, Literal, TypeVar, cast

from .connection import JsonRpcConnection
from .messages import JsonRpcId

StreamPolicy = Literal["never", "optional", "always"]
CancelDataFactory = Callable[[], object | Awaitable[object]]
EventT = TypeVar("EventT")
EventSerializer = Callable[[EventT], object]


class JsonRpcStreamSink(Generic[EventT]):
    """Event sink that serializes arbitrary application events into stream data."""

    __slots__ = ("_conn", "_serializer", "_stream_id")

    def __init__(
        self,
        conn: JsonRpcConnection,
        stream_id: JsonRpcId,
        serializer: EventSerializer[EventT],
    ) -> None:
        self._conn = conn
        self._stream_id = stream_id
        self._serializer = serializer

    async def emit(self, event: EventT) -> None:
        """Serialize and send one application event."""
        await self._conn.send_stream_data(self._stream_id, self._serializer(event))


class NullJsonRpcStreamSink(Generic[EventT]):
    """No-op sink for non-streaming calls that still accept an event sink."""

    __slots__ = ()

    async def emit(self, event: EventT) -> None:
        """Ignore an event for non-streaming execution."""


async def maybe_await(value: object | Awaitable[object]) -> object:
    """Return awaited values and pass through plain objects."""
    if inspect.isawaitable(value):
        return await cast(Awaitable[object], value)
    return value
