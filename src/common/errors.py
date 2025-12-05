"""Shared error codes and exceptions for backend agents."""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional


class ErrorCode(str, Enum):
    CONFIG_ERROR = "CONFIG_ERROR"
    SCHEMA_ERROR = "SCHEMA_ERROR"
    IO_ERROR = "IO_ERROR"
    STATE_ERROR = "STATE_ERROR"


class BackendError(RuntimeError):
    """Exception carrying a structured error code for CLI/agents."""

    def __init__(self, code: ErrorCode, message: str, *, context: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.code = code
        self.context = context or {}

    def __str__(self) -> str:  # pragma: no cover - formatting sugar
        base = super().__str__()
        return f"[{self.code}] {base}" if base else self.code.value
