"""State machine tracking long-running pipeline jobs."""
from __future__ import annotations

import threading
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from storage import record_job_event, upsert_job_status


class JobState(str, Enum):
    """Supported lifecycle states for backend jobs."""

    PENDING = "PENDING"
    ANALYZING = "ANALYZING"
    MAPPING = "MAPPING"
    MATERIALIZING = "MATERIALIZING"
    VALIDATING = "VALIDATING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_TERMINAL_STATES = {JobState.DONE, JobState.FAILED, JobState.CANCELLED}
_STATE_ORDER: Dict[JobState, int] = {
    JobState.PENDING: 0,
    JobState.ANALYZING: 1,
    JobState.MAPPING: 2,
    JobState.MATERIALIZING: 3,
    JobState.VALIDATING: 4,
    JobState.DONE: 5,
}


class JobStateMachine:
    """Thread-safe helper that records job state transitions to SQLite."""

    def __init__(
        self,
        job_id: str,
        sqlite_path: Optional[Path | str],
        *,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        self.job_id = job_id
        self._sqlite_path = Path(sqlite_path) if sqlite_path else None
        self._metadata = metadata or {}
        self._state = JobState.PENDING
        self._lock = threading.Lock()
        self._record(JobState.PENDING, detail="job registered")

    @property
    def state(self) -> JobState:
        return self._state

    def transition(self, target: JobState, *, detail: str | None = None) -> None:
        with self._lock:
            if target == self._state:
                return
            if not self._can_transition(target):
                raise ValueError(f"Invalid transition {self._state.value} -> {target.value}")
            self._state = target
            self._record(target, detail=detail)

    def mark_failed(self, detail: str | None = None) -> None:
        with self._lock:
            self._state = JobState.FAILED
            self._record(JobState.FAILED, detail=detail)

    def mark_cancelled(self, detail: str | None = None) -> None:
        with self._lock:
            self._state = JobState.CANCELLED
            self._record(JobState.CANCELLED, detail=detail)

    def _can_transition(self, target: JobState) -> bool:
        if self._state in _TERMINAL_STATES:
            return False
        if target in {JobState.FAILED, JobState.CANCELLED}:
            return True
        current_rank = _STATE_ORDER.get(self._state, -1)
        target_rank = _STATE_ORDER.get(target, -1)
        return target_rank >= current_rank

    def _record(self, state: JobState, detail: str | None) -> None:
        if not self._sqlite_path:
            return
        upsert_job_status(
            self._sqlite_path,
            self.job_id,
            state.value,
            detail=detail,
            metadata=self._metadata,
            last_error=detail if state == JobState.FAILED else None,
        )
        record_job_event(self._sqlite_path, self.job_id, state.value, detail)