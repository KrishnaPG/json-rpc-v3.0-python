"""Shared constants for the reusable JSON-RPC 3.0 package."""

from __future__ import annotations

from typing import Final

JSONRPC_VERSION: Final[str] = "3.0"
DEFAULT_QUEUE_SIZE: Final[int] = 256
DEFAULT_CANCEL_METHOD: Final[str] = "request.cancel"
