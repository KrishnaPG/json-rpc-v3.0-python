# jsonrpc3

`jsonrpc3` is a reusable [JSON-RPC 3.0](https://github.com/KrishnaPG/json-rpc-v3.0) package. It owns protocol mechanics so application modules do not repeat transport plumbing.

## What It Provides

- Message envelopes: requests, responses, acknowledgements, errors, and streams.
- Request parsing and validation.
- Registry-backed server dispatch.
- Typed params through Pydantic models.
- Reusable connection runtime with send locking, backpressure queue, task tracking, cancellation, flushing, and close lifecycle.
- Streaming runtime with acknowledgements, event sinks, terminal result frames, terminal error frames, delayed ack metadata, and cancellation error data hooks.
- Client request generation and direct response correlation.
- Optional FastAPI WebSocket adapter.
- Extension points for result serializers and exception mappers.

## What It Does Not Provide

- Application method names.
- Domain request/response models.
- Domain event schemas.
- Product, domain, workflow, or application-specific behavior.
- Durable business storage. Applications can plug storage into method handlers or future generic replay/cache contracts.

## When To Use

Use `jsonrpc3` whenever a module needs a JSON-RPC 3.0 server or client. Application code should register methods and call services; it should not parse JSON-RPC envelopes or manage stream task registries.

Do not use this package for non-JSON-RPC REST endpoints, internal Python function calls, or domain event modeling.

## Install

```bash
pip install fict-json-rpc
pip install "fict-json-rpc[fastapi]"  # optional FastAPI adapter
```

## Server Example

```python
from pydantic import BaseModel, ConfigDict, Field
from jsonrpc3 import JsonRpcContext, JsonRpcServer


class EchoParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(..., min_length=1)


server = JsonRpcServer()


@server.method("example.echo", params_model=EchoParams)
async def echo(ctx: JsonRpcContext, params: EchoParams) -> dict[str, str]:
    return {"text": params.text}
```

## Streaming Example

```python
@server.method("example.run", params_model=EchoParams, stream="optional")
async def run(ctx: JsonRpcContext, params: EchoParams) -> dict[str, str]:
    sink = ctx.optional_event_sink(lambda event: {"type": "event", "data": event})
    if sink is not None:
        await sink.emit({"started": params.text})
    return {"status": "done"}
```

The framework sends the ack, event frames, terminal result frame, cancellation error frame, and task cleanup. The method only supplies domain work and event serialization.

## Cancellation

`JsonRpcServer.cancel_method(...)` registers a normal JSON-RPC method such as `request.cancel`, but it is a control method, not a completion method. When cancellation succeeds, the cancel method does not emit a root-level `result`; the cancelled stream emits the terminal `stream.error` frame with code `-32800`.

Use `ctx.on_cancel(...)` only to attach application-specific data to that cancellation error. Do not use cancellation data as a successful stream result. Stream lifecycle frames carry identity only in `stream.id`; they must not include a root-level `id`.

Clients should call `abort_stream(stream_id)` for method-less abort or `cancel_stream(stream_id, method=...)` for application cancel methods. `cancel_stream(...)` sends a notification so the client waits on the original stream terminal frame, not on a separate cancel-method result.

## Optional FastAPI Adapter

`jsonrpc3.fastapi` is optional adapter glue. It adapts a FastAPI `WebSocket` to `JsonRpcServer`; it is not part of the core runtime contract.

```python
from fastapi import APIRouter, WebSocket
from jsonrpc3.fastapi import serve_fastapi_websocket

router = APIRouter()


@router.websocket("/rpc")
async def rpc(websocket: WebSocket) -> None:
    await serve_fastapi_websocket(websocket, server)
```

## Client Example

```python
from jsonrpc3 import JsonRpcClient

client = JsonRpcClient(send_json)
result = await client.request("example.echo", {"text": "hello"})
```

The transport still owns how frames are physically sent and received. Call `client.receive(payload)` for inbound response payloads.

## Stream Iterator Example

```python
stream = await client.stream("example.run", {"text": "hello"})

async for event in stream:
    handle_event(event)

result = await stream.result()
```

`stream(...)` requests `options.stream = true`, yields each `stream.data` payload by reference, follows ACK stream-id remaps, and ends only on terminal `stream.result` or `stream.error`. Use `await stream.abort()` for method-less aborts or `await stream.cancel(method="request.cancel")` for application cancel notifications.

## Limitations

- Stream iterators expose `stream.data` payloads directly; callers that need immutable snapshots must copy at the application boundary.
- Core modules are transport-neutral; use optional adapters or a custom send/receive wrapper for specific frameworks.
- Replay, cache, and idempotency are application-owned today. Add generic interfaces here only when a third application would otherwise repeat the same logic.
- Stream data, stream result, and stream error frames keep identity in the `stream` envelope and do not include root-level `id`.

## Acceptance Bar

A new JSON-RPC server should only create a server, register methods, attach a transport, and optionally provide serializers or exception mappers.

A new JSON-RPC client should only create a client, send requests, and feed received frames back into the client runtime.

Repeated parsers, dispatch loops, send locks, task registries, stream runners, cancellation handlers, or response builders belong in this package.
