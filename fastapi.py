"""Compatibility shim for the optional FastAPI JSON-RPC adapter."""

from .src.jsonrpc3.fastapi import serve_fastapi_websocket

__all__ = ["serve_fastapi_websocket"]
