"""Inbound JSON-RPC 3.0 parsing and validation helpers."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .constants import JSONRPC_VERSION
from .error_codes import JSONRPCErrorCode
from .errors import rpc_error
from .messages import UNSET, JsonRpcId, JSONRPCRequest, Unset


class TOptionsStream(BaseModel):
    """Schema-compatible request options for stream and abort control."""

    model_config = ConfigDict(extra="allow")

    stream: bool | JsonRpcId | None = None
    abort: bool | None = None


class InboundJSONRPCRequest(BaseModel):
    """Validated inbound request envelope shared by servers and clients."""

    model_config = ConfigDict(extra="forbid")

    jsonrpc: str = Field(default=JSONRPC_VERSION)
    method: str = Field(..., min_length=1)
    params: Any = UNSET
    options: TOptionsStream = Field(default_factory=TOptionsStream)
    id: JsonRpcId = Field(default=None)


class InboundJSONRPCAbortRequest(BaseModel):
    """Validated method-less stream abort request from the upstream schema."""

    model_config = ConfigDict(extra="forbid")

    jsonrpc: str = Field(default=JSONRPC_VERSION)
    options: TOptionsStream


def parse_request(payload: object) -> JSONRPCRequest:
    """Parse an inbound request payload into the domain-neutral request class."""
    if not isinstance(payload, dict):
        raise rpc_error(
            JSONRPCErrorCode.INVALID_REQUEST,
            "Invalid request",
            "Request body must be an object",
        )
    parsed = InboundJSONRPCRequest.model_validate(payload)
    if parsed.jsonrpc not in {"2.0", JSONRPC_VERSION}:
        raise rpc_error(
            JSONRPCErrorCode.INVALID_REQUEST,
            "Invalid request",
            "jsonrpc must be '2.0' or '3.0'",
        )
    msg_id: JsonRpcId | Unset = UNSET
    if "id" in parsed.model_fields_set:
        msg_id = parsed.id
    return JSONRPCRequest(
        method=parsed.method,
        params=parsed.params,
        options=parsed.options.model_dump(exclude_none=True),
        msg_id=msg_id,
        jsonrpc=parsed.jsonrpc,
    )


def parse_abort_request(payload: object) -> JsonRpcId | None:
    """Return the target stream id for a schema-style abort request."""
    if not isinstance(payload, dict):
        return None
    if "method" in payload:
        return None
    parsed = InboundJSONRPCAbortRequest.model_validate(payload)
    if parsed.jsonrpc != JSONRPC_VERSION:
        raise rpc_error(
            JSONRPCErrorCode.INVALID_REQUEST,
            "Invalid request",
            f"abort requests require jsonrpc '{JSONRPC_VERSION}'",
        )
    if parsed.options.abort is not True:
        return None
    return parsed.options.stream
