"""Centralized resource budgeting for backend jobs."""
from __future__ import annotations

import math
import shutil
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from common.models import ResourceLimits


class ResourceLimitError(RuntimeError):
    """Raised when a reservation would exceed configured budgets."""


@dataclass(slots=True)
class ResourceLease:
    manager: "ResourceManager"
    memory_mb: int = 0
    disk_mb: int = 0
    workers: int = 0
    _released: bool = False

    def release(self) -> None:
        if self._released:
            return
        self.manager._release(self.memory_mb, self.disk_mb, self.workers)
        self._released = True

    def __enter__(self) -> "ResourceLease":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.release()


class ResourceManager:
    """Tracks RAM/disk/workers budgets and temp directories for jobs."""

    def __init__(self, limits: Optional[ResourceLimits] = None) -> None:
        self.limits = limits or ResourceLimits()
        self._lock = threading.Lock()
        self._memory_in_use = 0
        self._disk_in_use = 0
        self._workers_in_use = 0
        self._temp_root = Path(self.limits.temp_dir or ResourceLimits().temp_dir).expanduser()
        self._temp_root.mkdir(parents=True, exist_ok=True)

    def plan_workers(self, requested: int) -> int:
        requested = max(1, requested)
        limit = self.limits.max_workers
        if limit is None or limit <= 0:
            return requested
        return max(1, min(requested, limit))

    def reserve(
        self,
        *,
        memory_mb: int = 0,
        disk_mb: int = 0,
        workers: int = 0,
    ) -> ResourceLease:
        memory_mb = max(0, int(memory_mb))
        disk_mb = max(0, int(disk_mb))
        workers = max(0, int(workers))
        with self._lock:
            self._ensure_capacity(memory_mb, disk_mb, workers)
            self._memory_in_use += memory_mb
            self._disk_in_use += disk_mb
            self._workers_in_use += workers
        return ResourceLease(self, memory_mb, disk_mb, workers)

    def scratch_dir(self, job_id: str, *segments: str) -> Path:
        """Return/create a stable subdirectory for temporary files."""

        path = self._temp_root / _sanitize_segment(job_id or "job")
        for segment in segments:
            if not segment:
                continue
            path /= _sanitize_segment(str(segment))
        path.mkdir(parents=True, exist_ok=True)
        return path

    def cleanup(self, job_id: str) -> None:
        target = self._temp_root / _sanitize_segment(job_id or "job")
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)

    def available_memory_mb(self) -> Optional[int]:
        if self.limits.memory_mb is None:
            return None
        return max(0, self.limits.memory_mb - self._memory_in_use)

    def available_disk_mb(self) -> Optional[int]:
        if self.limits.spill_mb is None:
            return None
        return max(0, self.limits.spill_mb - self._disk_in_use)

    def available_workers(self) -> Optional[int]:
        if self.limits.max_workers is None:
            return None
        return max(0, self.limits.max_workers - self._workers_in_use)

    def disk_mb_from_bytes(self, byte_count: int) -> int:
        if byte_count <= 0:
            return 0
        return max(1, math.ceil(byte_count / (1024 * 1024)))

    # Internal helpers -------------------------------------------------

    def _ensure_capacity(self, memory_mb: int, disk_mb: int, workers: int) -> None:
        if self.limits.memory_mb is not None:
            if self._memory_in_use + memory_mb > self.limits.memory_mb:
                raise ResourceLimitError(
                    f"RAM budget exceeded: requested {memory_mb} MB, "
                    f"available {self.available_memory_mb()} MB"
                )
        if self.limits.spill_mb is not None:
            if self._disk_in_use + disk_mb > self.limits.spill_mb:
                raise ResourceLimitError(
                    f"Disk spill budget exceeded: requested {disk_mb} MB, "
                    f"available {self.available_disk_mb()} MB"
                )
        if self.limits.max_workers is not None:
            if self._workers_in_use + workers > self.limits.max_workers:
                raise ResourceLimitError(
                    f"Worker budget exceeded: requested {workers}, "
                    f"available {self.available_workers()}"
                )

    def _release(self, memory_mb: int, disk_mb: int, workers: int) -> None:
        with self._lock:
            self._memory_in_use = max(0, self._memory_in_use - memory_mb)
            self._disk_in_use = max(0, self._disk_in_use - disk_mb)
            self._workers_in_use = max(0, self._workers_in_use - workers)


def _sanitize_segment(value: str) -> str:
    if not value:
        return "segment"
    cleaned = [ch.lower() if ch.isalnum() else "-" for ch in value.strip()]
    slug = "".join(cleaned).strip("-")
    return slug or "segment"
