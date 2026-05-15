"""Compatibility shim for reusable JSON-RPC server primitives."""

from .src.jsonrpc3.server import JsonRpcContext, JsonRpcServer, default_result_serializer

__all__ = ["JsonRpcContext", "JsonRpcServer", "default_result_serializer"]
