"""Optional FastAPI WebSocket adapter for the reusable JSON-RPC runtime.

This module is intentionally thin and optional: it adapts FastAPI's WebSocket
shape to ``JsonRpcServer`` without making the core runtime depend on a web
framework.
"""

from __future__ import annotations

from fastapi import WebSocket, WebSocketDisconnect

from .connection import JsonRpcConnection
from .server import JsonRpcServer


async def serve_fastapi_websocket(
    websocket: WebSocket,
    server: JsonRpcServer,
    *,
    queue_size: int = 256,
) -> None:
    """Serve a JSON-RPC server over an optional FastAPI WebSocket transport."""
    await websocket.accept()
    conn = JsonRpcConnection(websocket.send_json, queue_size=queue_size)
    try:
        while True:
            await server.handle(conn, await websocket.receive_json())
    except WebSocketDisconnect:
        await conn.cancel_all()
    finally:
        await conn.close()
