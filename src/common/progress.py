"""Structured progress logging utilities."""
from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Optional

from .models import FileProgress


class ProgressLogger:
    """Writes progress events to JSONL for later inspection."""

    def __init__(self, path: Optional[Path]) -> None:
        self.path = path
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, progress: FileProgress) -> None:
        if not self.path:
            return
        payload = asdict(progress)
        payload["file_path"] = str(progress.file_path)
        payload["timestamp"] = time.time()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload))
            handle.write("\n")


class BenchmarkRecorder:
    """Stores throughput measurements for later analysis."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, dataset: str, metrics: dict) -> None:
        payload = {"dataset": dataset, **metrics, "timestamp": time.time()}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload))
            handle.write("\n")
