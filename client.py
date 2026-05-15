"""Compatibility shim for reusable JSON-RPC client primitives."""

from .src.jsonrpc3.client import JsonRpcClient, JsonRpcClientError, JsonRpcStreamCall

__all__ = ["JsonRpcClient", "JsonRpcClientError", "JsonRpcStreamCall"]
