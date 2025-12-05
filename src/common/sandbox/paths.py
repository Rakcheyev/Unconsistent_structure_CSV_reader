"""Sandboxed path helpers to prevent directory escape."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from common.errors import BackendError, ErrorCode


class SandboxViolation(BackendError):
    """Raised when a requested path would escape the sandbox root."""

    def __init__(self, *, attempted: Path, root: Path, reason: str) -> None:
        super().__init__(
            ErrorCode.STATE_ERROR,
            f"Sandbox violation: {reason} (attempted={attempted}, root={root})",
            context={"attempted": str(attempted), "root": str(root)},
        )


@dataclass(slots=True)
class Sandbox:
    """Resolves relative job paths within a fixed root."""

    root: Path
    allowlist: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        self.root = self._normalize(self.root)
        self.allowlist = tuple(self._normalize(path) for path in self.allowlist)

    def resolve(self, *segments: str | os.PathLike[str], must_exist: bool = False) -> Path:
        """Resolve segments inside the sandbox root, rejecting escapes."""

        candidate = self._normalize(self.root.joinpath(*segments))
        if not self._is_allowed(candidate):
            raise SandboxViolation(attempted=candidate, root=self.root, reason="outside sandbox")
        if must_exist and not candidate.exists():
            raise SandboxViolation(attempted=candidate, root=self.root, reason="path does not exist")
        return candidate

    def ensure_dir(self, *segments: str | os.PathLike[str]) -> Path:
        target = self.resolve(*segments)
        target.mkdir(parents=True, exist_ok=True)
        return target

    def child(self, *segments: str | os.PathLike[str]) -> "Sandbox":
        return Sandbox(self.resolve(*segments), allowlist=self.allowlist)

    def _normalize(self, path: Path | os.PathLike[str]) -> Path:
        resolved = Path(path).expanduser().resolve()
        return resolved

    def _is_allowed(self, target: Path) -> bool:
        if _is_relative_to(target, self.root):
            return True
        return any(_is_relative_to(target, allowed) for allowed in self.allowlist)


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
