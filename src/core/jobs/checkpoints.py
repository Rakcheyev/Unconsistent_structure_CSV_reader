"""Lightweight checkpoint registry backed by JSON files per job/phase."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict


class CheckpointRegistry:
    """Stores checkpoint payloads as JSON per job_id/phase (thread-safe)."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else Path("artifacts/checkpoints")
        self._lock = threading.Lock()

    def load(self, job_id: str, phase: str) -> Dict[str, Any]:
        path = self._path(job_id, phase)
        with self._lock:
            if not path.exists():
                return {}
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return {}
        return data if isinstance(data, dict) else {}

    def save(self, job_id: str, phase: str, payload: Dict[str, Any]) -> None:
        path = self._path(job_id, phase)
        enriched = dict(payload)
        enriched["updated_at"] = time.time()
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(enriched, indent=2, ensure_ascii=False), encoding="utf-8")

    def clear(self, job_id: str, phase: str) -> None:
        path = self._path(job_id, phase)
        with self._lock:
            path.unlink(missing_ok=True)

    def _path(self, job_id: str, phase: str) -> Path:
        safe_phase = phase.replace("/", "_")
        safe_job = job_id.replace(os.sep, "_")
        return self.base_dir / safe_phase / f"{safe_job}.json"
