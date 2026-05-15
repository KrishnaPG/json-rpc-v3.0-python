"""Reusable JSON-RPC 3.0 server runtime.

The server layer is intentionally domain-neutral: applications register typed
methods, serializers, and exception mappers; this package owns protocol
dispatch, streaming, cancellation, and response envelopes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar, cast

from pydantic import BaseModel

from .connection import JsonRpcConnection
from .error_codes import JSONRPCErrorCode
from .errors import (
    ExceptionMapper,
    default_exception_mapper,
    error_response,
    rpc_error,
)
from .messages import UNSET, JSONRPCError, JsonRpcId, JSONRPCRequest, Unset
from .parsing import parse_abort_request, parse_request
from .streaming import (
    CancelDataFactory,
    EventSerializer,
    EventT,
    JsonRpcStreamSink,
    NullJsonRpcStreamSink,
    StreamPolicy,
    maybe_await,
)

MethodHandler = Callable[["JsonRpcContext", object], Awaitable[object]]
ResultSerializer = Callable[[object], object]
ParamsT = TypeVar("ParamsT", bound=object)


class _NoMethodResponse:
    """Sentinel for control methods whose observable result is another frame."""

    __slots__ = ()


NO_METHOD_RESPONSE = _NoMethodResponse()


class JsonRpcContext:
    """Per-request context passed to JSON-RPC method handlers.

    Handlers use this object to acknowledge work, emit stream events, register
    cancellation aliases, and serialize final results through server policy.
    """

    __slots__ = (
        "_cancel_data",
        "_conn",
        "_is_streaming",
        "_request",
        "_result_serializer",
        "_stream_id",
    )

    def __init__(
        self,
        conn: JsonRpcConnection,
        request: JSONRPCRequest,
        *,
        is_streaming: bool,
        stream_id: JsonRpcId,
        result_serializer: ResultSerializer,
    ) -> None:
        self._conn = conn
        self._request = request
        self._is_streaming = is_streaming
        self._stream_id = stream_id
        self._result_serializer = result_serializer
        self._cancel_data: CancelDataFactory | None = None

    @property
    def request(self) -> JSONRPCRequest:
        """Return the parsed request envelope.

        Use this for request metadata such as `id`, `options`, or method name;
        do not mutate it while the server is dispatching the request.
        """
        return self._request

    @property
    def is_streaming(self) -> bool:
        """Return whether this request is currently streaming.

        Use this when a handler supports both direct and streaming calls and
        needs to choose whether to emit intermediate events.
        """
        return self._is_streaming

    @property
    def stream_id(self) -> JsonRpcId:
        """Return the active stream id for streaming handlers.

        Expect a JSON-RPC invalid-request error if the current request did not
        negotiate streaming; check `is_streaming` first for optional streams.
        """
        if self._stream_id is None:
            raise rpc_error(
                JSONRPCErrorCode.INVALID_REQUEST,
                "Invalid request",
                "Streaming context requires an id",
            )
        return self._stream_id

    async def ack(self, *, stream: dict[str, object] | None = None) -> None:
        """Send an ACK for direct or streaming work.

        Direct ACKs use root `id`; stream ACKs put identity in `ack.id` and may
        include stream metadata such as a remapped private `stream.id`.
        """
        if not self._is_streaming:
            await self._ack_direct()
            return
        await self._ack_stream(stream)

    def event_sink(
        self,
        serializer: EventSerializer[EventT],
    ) -> JsonRpcStreamSink[EventT] | NullJsonRpcStreamSink[EventT]:
        """Return an event sink for handlers that always call `emit`.

        Streaming requests send serialized events as `stream.data`; direct
        requests receive a no-op sink so the same handler can run both modes.
        """
        if self._is_streaming:
            return JsonRpcStreamSink(self._conn, self.stream_id, serializer)
        return NullJsonRpcStreamSink()

    def optional_event_sink(
        self,
        serializer: EventSerializer[EventT],
    ) -> JsonRpcStreamSink[EventT] | None:
        """Return a stream sink only when streaming is active.

        Use this when direct requests should skip event generation entirely
        instead of paying serialization cost for ignored events.
        """
        if self._is_streaming:
            return JsonRpcStreamSink(self._conn, self.stream_id, serializer)
        return None

    def track_alias(self, key: JsonRpcId) -> None:
        """Register the current stream task under an application alias.

        Use this after an ACK exposes a domain id, so later cancel requests can
        target that alias while the stream still terminates under `stream.id`.
        """
        self._conn.track_current(key)

    def cancel(self, key: JsonRpcId) -> bool:
        """Cancel a task tracked by request id or application alias.

        Returns `True` when a live task was found; successful cancellation is
        reported to clients by the target stream's terminal error frame.
        """
        return self._conn.cancel(key)

    def on_cancel(self, factory: CancelDataFactory) -> None:
        """Attach optional application data to stream cancellation errors.

        The factory runs when cancellation is observed and its value becomes
        `error.data`; it is never emitted as a successful `result`.
        """
        self._cancel_data = factory

    async def cancelled_data(self) -> object | None:
        """Return serialized application data for a cancellation error.

        Framework code calls this while building the terminal `stream.error`;
        application handlers normally register data through `on_cancel`.
        """
        if self._cancel_data is None:
            return None
        return self._result_serializer(await maybe_await(self._cancel_data()))

    def serialize_result(self, result: object) -> object:
        """Serialize one method result using the server-level serializer.

        Use this only from framework or advanced adapters that need the same
        Pydantic-aware result shape as normal method dispatch.
        """
        return self._result_serializer(result)

    async def _ack_direct(self) -> None:
        if not self._request.has_id:
            return
        await self._conn.send_method_ack(self._request.id)

    async def _ack_stream(self, stream: dict[str, object] | None) -> None:
        next_stream_id = _stream_metadata_id(stream)
        await self._conn.send_ack(self.stream_id, stream=stream)
        if next_stream_id is None:
            return
        self._stream_id = next_stream_id
        self._conn.track_current(next_stream_id)


class JsonRpcServer:
    """Registry-backed JSON-RPC 3.0 server with reusable streaming support.

    Applications register methods and pass inbound payloads to `handle`; the
    server owns validation, dispatch, stream frames, cancellation, and errors.
    """

    __slots__ = ("_exception_mappers", "_methods", "_result_serializer")

    def __init__(
        self,
        *,
        exception_mappers: Sequence[ExceptionMapper] = (),
        result_serializer: ResultSerializer | None = None,
    ) -> None:
        """Create an empty server registry.

        Supply exception mappers for domain errors and a serializer when method
        results need conversion before becoming JSON-RPC `result` values.
        """
        self._methods: dict[str, _MethodSpec] = {}
        self._exception_mappers = tuple(exception_mappers)
        self._result_serializer = result_serializer or default_result_serializer

    def method(
        self,
        name: str,
        *,
        params_model: type[BaseModel] | None = None,
        stream: StreamPolicy = "never",
        auto_ack: bool = True,
    ) -> Callable[
        [Callable[[JsonRpcContext, ParamsT], Awaitable[object]]],
        Callable[[JsonRpcContext, ParamsT], Awaitable[object]],
    ]:
        """Register an application method without transport boilerplate.

        Decorate an async handler that accepts `(ctx, params)`; `params_model`
        validates payloads, and `stream` controls direct versus stream frames.
        """

        def decorator(
            handler: Callable[[JsonRpcContext, ParamsT], Awaitable[object]],
        ) -> Callable[[JsonRpcContext, ParamsT], Awaitable[object]]:
            self._methods[name] = _MethodSpec(
                name,
                cast(MethodHandler, handler),
                params_model,
                stream,
                auto_ack,
            )
            return handler

        return decorator

    def cancel_method(
        self,
        name: str,
        *,
        target_param: str = "id",
    ) -> Callable[[type[BaseModel]], type[BaseModel]]:
        """Register a control method that terminates the target stream.

        The decorated params model must expose `target_param`; successful
        cancellation emits no method result and completes the stream with error.
        """

        def decorator(params_model: type[BaseModel]) -> type[BaseModel]:
            async def cancel(ctx: JsonRpcContext, params: object) -> object:
                target = str(getattr(params, target_param))
                if not ctx.cancel(target):
                    raise rpc_error(
                        JSONRPCErrorCode.RESOURCE_NOT_FOUND,
                        "Request not found",
                        f"Active request '{target}' not found",
                    )
                return NO_METHOD_RESPONSE

            self._methods[name] = _MethodSpec(name, cancel, params_model, "never", True)
            return params_model

        return decorator

    async def handle(self, conn: JsonRpcConnection, payload: object) -> None:
        """Parse, dispatch, and respond to one inbound JSON-RPC payload.

        Call this once per received message object; it also handles method-less
        stream abort requests and flushes queued outbound frames before return.
        """
        request: JSONRPCRequest | None = None
        try:
            if await self._handle_abort(conn, payload):
                return
            request = parse_request(payload)
            spec = self._method_for(request)
            params = spec.validate_params(request.params)
            ctx = self._context(conn, request, spec)
            await self._dispatch(conn, spec, ctx, params)
        except BaseException as exc:
            if _suppress_error_response(request):
                return
            await conn.send(error_response(self._map_exception(exc), _request_id(request)))
        finally:
            await conn.flush()

    async def _handle_abort(self, conn: JsonRpcConnection, payload: object) -> bool:
        stream_id = parse_abort_request(payload)
        if stream_id is None:
            return False
        conn.cancel(stream_id)
        return True

    def _method_for(self, request: JSONRPCRequest) -> _MethodSpec:
        spec = self._methods.get(request.method)
        if spec is None:
            raise rpc_error(
                JSONRPCErrorCode.METHOD_NOT_FOUND,
                "Method not found",
                f"Unknown method: {request.method}",
            )
        return spec

    def _context(
        self,
        conn: JsonRpcConnection,
        request: JSONRPCRequest,
        spec: _MethodSpec,
    ) -> JsonRpcContext:
        is_streaming = _stream_requested(spec.stream, request)
        stream_id = _optional_stream_id(request, is_streaming)
        return JsonRpcContext(
            conn,
            request,
            is_streaming=is_streaming,
            stream_id=stream_id,
            result_serializer=self._result_serializer,
        )

    async def _dispatch(
        self,
        conn: JsonRpcConnection,
        spec: _MethodSpec,
        ctx: JsonRpcContext,
        params: object,
    ) -> None:
        dispatchers = {
            True: self._dispatch_stream,
            False: self._dispatch_direct,
        }
        await dispatchers[ctx.is_streaming](conn, spec, ctx, params)

    async def _dispatch_direct(
        self,
        conn: JsonRpcConnection,
        spec: _MethodSpec,
        ctx: JsonRpcContext,
        params: object,
    ) -> None:
        result = await spec.handler(ctx, params)
        if result is NO_METHOD_RESPONSE:
            return
        if not ctx.request.has_id:
            return
        await conn.send_result(ctx.serialize_result(result), ctx.request.id)

    async def _dispatch_stream(
        self,
        conn: JsonRpcConnection,
        spec: _MethodSpec,
        ctx: JsonRpcContext,
        params: object,
    ) -> None:
        if spec.auto_ack:
            await ctx.ack()
        task = asyncio.create_task(self._run_stream(conn, spec, ctx, params))
        conn.track(ctx.stream_id, task)

    async def _run_stream(
        self,
        conn: JsonRpcConnection,
        spec: _MethodSpec,
        ctx: JsonRpcContext,
        params: object,
    ) -> None:
        task = asyncio.current_task()
        try:
            result = await spec.handler(ctx, params)
            await conn.send_stream_result(ctx.stream_id, ctx.serialize_result(result))
        except asyncio.CancelledError:
            await self._send_cancelled_stream(conn, ctx)
            raise
        except BaseException as exc:
            await conn.send_stream_error(ctx.stream_id, self._map_exception(exc))
        finally:
            await conn.flush()
            if task is not None:
                conn.untrack_task(task)

    async def _send_cancelled_stream(self, conn: JsonRpcConnection, ctx: JsonRpcContext) -> None:
        data = await ctx.cancelled_data()
        await conn.send_stream_error(
            ctx.stream_id,
            JSONRPCError(
                code=int(JSONRPCErrorCode.REQUEST_CANCELLED),
                title="Request cancelled",
                message=f"Request '{ctx.stream_id}' was cancelled",
                data={True: data, False: UNSET}[data is not None],
            ),
        )

    def _map_exception(self, exc: BaseException) -> JSONRPCError:
        for mapper in (*self._exception_mappers, default_exception_mapper):
            mapped = mapper(exc)
            if mapped is not None:
                return mapped
        return JSONRPCError(
            code=int(JSONRPCErrorCode.INTERNAL_ERROR),
            title="Internal error",
            message=str(exc),
        )


class _MethodSpec:
    """Internal method registry entry."""

    __slots__ = ("auto_ack", "handler", "name", "params_model", "stream")

    def __init__(
        self,
        name: str,
        handler: MethodHandler,
        params_model: type[BaseModel] | None,
        stream: StreamPolicy,
        auto_ack: bool,
    ) -> None:
        self.name: str = name
        self.handler: MethodHandler = handler
        self.params_model: type[BaseModel] | None = params_model
        self.stream: StreamPolicy = stream
        self.auto_ack: bool = auto_ack

    def validate_params(self, params: object) -> object:
        """Validate raw params with the registered Pydantic model when present."""
        if self.params_model is None:
            return params
        return self.params_model.model_validate(params)


def default_result_serializer(result: object) -> object:
    """Serialize Pydantic-like results without requiring application glue.

    Servers use this by default so handlers may return Pydantic models directly;
    non-Pydantic objects pass through unchanged.
    """
    dumper = getattr(result, "model_dump", None)
    if callable(dumper):
        return dumper(mode="json")
    return result


def _stream_requested(policy: StreamPolicy, request: JSONRPCRequest) -> bool:
    selectors: dict[StreamPolicy, Callable[[], bool]] = {
        "never": lambda: False,
        "optional": lambda: bool(request.options.get("stream")),
        "always": lambda: True,
    }
    return selectors[policy]()


def _stream_id(request: JSONRPCRequest) -> JsonRpcId:
    if not request.has_id:
        raise rpc_error(
            JSONRPCErrorCode.INVALID_REQUEST,
            "Invalid request",
            "Streaming requests require an id",
        )
    return request.id


def _optional_stream_id(request: JSONRPCRequest, is_streaming: bool) -> JsonRpcId:
    if is_streaming:
        return _stream_id(request)
    return None


def _request_id(request: JSONRPCRequest | None) -> JsonRpcId | Unset:
    if request is None:
        return UNSET
    return request.id


def _suppress_error_response(request: JSONRPCRequest | None) -> bool:
    if request is None:
        return False
    return not request.has_id


def _stream_metadata_id(stream: dict[str, object] | None) -> JsonRpcId:
    if stream is None:
        return None
    stream_id = stream.get("id")
    if isinstance(stream_id, str | int | float) or stream_id is None:
        return stream_id
    return None
