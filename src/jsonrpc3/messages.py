"""Domain-neutral JSON-RPC 3.0 message classes.

These classes are intentionally transport- and domain-agnostic so they can be
reused by any JSON-RPC 3.0 integration.
"""

from __future__ import annotations

from typing import Any

from .constants import JSONRPC_VERSION

JsonRpcId = str | int | float | None


class _Unset:
    """Sentinel that distinguishes omitted fields from explicit JSON null."""

    __slots__ = ()


UNSET = _Unset()
Unset = _Unset


class JSONRPCRequest:
    """JSON-RPC 3.0 request message."""

    __slots__ = ("jsonrpc", "method", "params", "options", "id", "has_id")

    def __init__(
        self,
        method: str,
        params: Any = UNSET,
        options: dict[str, Any] | None = None,
        msg_id: JsonRpcId | Unset = UNSET,
        *,
        jsonrpc: str = JSONRPC_VERSION,
    ) -> None:
        self.jsonrpc: str = jsonrpc
        self.method: str = method
        self.params: Any = params
        self.options: dict[str, Any] = options or {}
        self.has_id = not isinstance(msg_id, _Unset)
        self.id: JsonRpcId | None = None
        if not isinstance(msg_id, _Unset):
            self.id = msg_id

    def to_dict(self) -> dict[str, Any]:
        """Return the request as a JSON-serializable dictionary."""
        result: dict[str, Any] = {
            "jsonrpc": self.jsonrpc,
            "method": self.method,
            "options": self.options,
        }
        if not isinstance(self.params, _Unset):
            result["params"] = self.params
        if self.has_id:
            result["id"] = self.id
        return result


class JSONRPCAck:
    """JSON-RPC 3.0 acknowledgement."""

    __slots__ = (
        "id",
        "job_id",
        "step_id",
        "step_instance_id",
        "progress",
        "stage",
        "metrics",
        "stream",
    )

    def __init__(
        self,
        msg_id: JsonRpcId = None,
        job_id: str | None = None,
        step_id: str | None = None,
        step_instance_id: str | None = None,
        progress: int | None = None,
        stage: str | None = None,
        metrics: dict[str, Any] | None = None,
        stream: dict[str, Any] | None = None,
    ) -> None:
        self.id = msg_id
        self.job_id = job_id
        self.step_id = step_id
        self.step_instance_id = step_instance_id
        self.progress = progress
        self.stage = stage
        self.metrics = metrics or {}
        self.stream = stream

    def to_dict(self) -> dict[str, Any]:
        """Return the acknowledgement as a JSON-serializable dictionary."""
        result: dict[str, Any] = {}
        if self.id is not None:
            result["id"] = self.id
        if self.job_id is not None:
            result["jobId"] = self.job_id
        if self.step_id is not None:
            result["stepId"] = self.step_id
        if self.step_instance_id is not None:
            result["stepInstanceId"] = self.step_instance_id
        if self.progress is not None:
            result["progress"] = self.progress
        if self.stage is not None:
            result["stage"] = self.stage
        if self.metrics:
            result["metrics"] = self.metrics
        if self.stream is not None:
            result["stream"] = self.stream
        return result


class JSONRPCError:
    """JSON-RPC 3.0 structured error."""

    __slots__ = ("code", "title", "message", "data")

    def __init__(
        self,
        code: int,
        title: str,
        message: str = "",
        data: Any = UNSET,
    ) -> None:
        self.code = code
        self.title = title
        self.message = message
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        """Return the error as a JSON-serializable dictionary."""
        result: dict[str, Any] = {
            "code": self.code,
            "title": self.title,
            "message": self.message,
        }
        if not isinstance(self.data, _Unset):
            result["data"] = self.data
        return result


class JSONRPCStream:
    """JSON-RPC 3.0 stream envelope."""

    __slots__ = ("id", "data")

    def __init__(self, stream_id: JsonRpcId, data: Any = UNSET) -> None:
        self.id = stream_id
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        """Return the stream envelope as a JSON-serializable dictionary."""
        result: dict[str, Any] = {"id": self.id}
        if not isinstance(self.data, _Unset):
            result["data"] = self.data
        return result


class JSONRPCResponse:
    """JSON-RPC 3.0 response envelope."""

    __slots__ = ("jsonrpc", "result", "ack", "error", "stream", "id")

    def __init__(
        self,
        result: Any = UNSET,
        ack: JSONRPCAck | dict[str, Any] | None | Unset = UNSET,
        error: JSONRPCError | Unset = UNSET,
        stream: JSONRPCStream | None = None,
        msg_id: JsonRpcId | Unset = UNSET,
        *,
        jsonrpc: str = JSONRPC_VERSION,
    ) -> None:
        self.jsonrpc: str = jsonrpc
        self.result = result
        self.ack = ack
        self.error = error
        self.stream = stream
        self.id = msg_id

    def to_dict(self) -> dict[str, Any]:
        """Return the response as a JSON-serializable dictionary."""
        result: dict[str, Any] = {"jsonrpc": self.jsonrpc}
        if self.stream is None and not isinstance(self.id, _Unset):
            result["id"] = self.id
        if not isinstance(self.result, _Unset):
            result["result"] = self.result
        if not isinstance(self.ack, _Unset):
            result["ack"] = self.ack
            if isinstance(self.ack, JSONRPCAck):
                result["ack"] = self.ack.to_dict()
        if not isinstance(self.error, _Unset):
            result["error"] = self.error.to_dict()
        if self.stream is not None:
            result["stream"] = self.stream.to_dict()
        return result


class JSONRPCMessage:
    """Union type for inbound JSON-RPC 3.0 messages."""

    __slots__ = (
        "jsonrpc",
        "method",
        "params",
        "options",
        "ack",
        "result",
        "error",
        "stream",
        "id",
    )

    def __init__(
        self,
        method: str | None = None,
        params: Any = UNSET,
        options: dict[str, Any] | None = None,
        ack: JSONRPCAck | None = None,
        result: Any = UNSET,
        error: JSONRPCError | None = None,
        stream: JSONRPCStream | None = None,
        msg_id: JsonRpcId | Unset = UNSET,
    ) -> None:
        self.jsonrpc: str = JSONRPC_VERSION
        self.method = method
        self.params = params
        self.options = options or {}
        self.ack = ack
        self.result = result
        self.error = error
        self.stream = stream
        self.id = msg_id
