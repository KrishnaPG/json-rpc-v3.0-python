"""Compatibility shim for reusable JSON-RPC error helpers."""

from .src.jsonrpc3.errors import (
    ExceptionMapper,
    JsonRpcProtocolError,
    default_exception_mapper,
    error_response,
    map_exception_type,
    rpc_error,
)

__all__ = [
    "ExceptionMapper",
    "JsonRpcProtocolError",
    "default_exception_mapper",
    "error_response",
    "map_exception_type",
    "rpc_error",
]
