"""Reusable JSON-RPC 3.0 error helpers and exception mapping."""

from __future__ import annotations

from collections.abc import Callable

from pydantic import ValidationError

from .error_codes import JSONRPCErrorCode
from .messages import JSONRPCError, JsonRpcId, JSONRPCResponse, Unset

ExceptionMapper = Callable[[BaseException], JSONRPCError | None]


class JsonRpcProtocolError(Exception):
    """Exception that carries a JSON-RPC error envelope."""

    __slots__ = ("error",)

    def __init__(self, error: JSONRPCError) -> None:
        super().__init__(error.message)
        self.error = error


def rpc_error(code: int | JSONRPCErrorCode, title: str, message: str) -> JsonRpcProtocolError:
    """Create an exception with a reusable JSON-RPC error payload."""
    return JsonRpcProtocolError(JSONRPCError(code=int(code), title=title, message=message))


def error_response(error: JSONRPCError, msg_id: JsonRpcId | Unset) -> dict[str, object]:
    """Serialize one JSON-RPC error response."""
    return JSONRPCResponse(error=error, msg_id=msg_id).to_dict()


def map_exception_type(
    exc_type: type[BaseException],
    code: int | JSONRPCErrorCode,
    title: str,
) -> ExceptionMapper:
    """Build a mapper for application exception classes without binding domains."""

    def mapper(exc: BaseException) -> JSONRPCError | None:
        if not isinstance(exc, exc_type):
            return None
        return JSONRPCError(code=int(code), title=title, message=str(exc))

    return mapper


def default_exception_mapper(exc: BaseException) -> JSONRPCError | None:
    """Map common framework exceptions to protocol-level errors."""
    mappers: tuple[ExceptionMapper, ...] = (
        _jsonrpc_exception,
        _validation_error,
        _key_error,
        _value_error,
    )
    for mapper in mappers:
        mapped = mapper(exc)
        if mapped is not None:
            return mapped
    return None


def _jsonrpc_exception(exc: BaseException) -> JSONRPCError | None:
    if not isinstance(exc, JsonRpcProtocolError):
        return None
    return exc.error


def _validation_error(exc: BaseException) -> JSONRPCError | None:
    if not isinstance(exc, ValidationError):
        return None
    errors = exc.errors()
    detail = str(exc)
    if errors:
        detail = str(errors[0]["msg"])
    return JSONRPCError(
        code=int(JSONRPCErrorCode.INVALID_PARAMS),
        title="Invalid params",
        message=detail,
    )


def _key_error(exc: BaseException) -> JSONRPCError | None:
    if not isinstance(exc, KeyError):
        return None
    return JSONRPCError(
        code=int(JSONRPCErrorCode.RESOURCE_NOT_FOUND),
        title="Resource not found",
        message=str(exc),
    )


def _value_error(exc: BaseException) -> JSONRPCError | None:
    if not isinstance(exc, ValueError):
        return None
    return JSONRPCError(
        code=int(JSONRPCErrorCode.REQUEST_CONFLICT),
        title="Request conflict",
        message=str(exc),
    )
