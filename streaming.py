"""Compatibility shim for reusable JSON-RPC streaming primitives."""

from .src.jsonrpc3.streaming import JsonRpcStreamSink, NullJsonRpcStreamSink

__all__ = ["JsonRpcStreamSink", "NullJsonRpcStreamSink"]
