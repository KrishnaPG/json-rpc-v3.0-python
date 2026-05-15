"""Domain-neutral JSON-RPC 3.0 error code definitions."""

from __future__ import annotations

from enum import IntEnum


class JSONRPCErrorCode(IntEnum):
    """Reusable JSON-RPC 3.0 error codes used by transports and adapters."""

    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    REQUEST_CANCELLED = -32800
    UNAUTHENTICATED = -32001
    FORBIDDEN = -32003
    RESOURCE_NOT_FOUND = -32004
    METHOD_NOT_SUPPORTED = -32005
    TIMEOUT = -32008
    REQUEST_CONFLICT = -32009
    PRECONDITION_FAILED = -32012
    PAYLOAD_TOO_LARGE = -32013
    TOO_MANY_REQUESTS = -32029
    CONNECTION_FAILURE = -32030
    STREAM_ABORTED_SCHEMA_CODE = 8000
