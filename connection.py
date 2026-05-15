"""Compatibility shim for reusable JSON-RPC connection primitives."""

from .src.jsonrpc3.connection import JsonRpcConnection, JsonSender

__all__ = ["JsonRpcConnection", "JsonSender"]
