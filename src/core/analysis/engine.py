"""Phase 1 analysis implementation with resource-aware limits."""
from __future__ import annotations

import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from common.config import error_mode_from_policy
from common.models import (
    ColumnStats,
    FileAnalysisResult,
    FileBlock,
    FileProgress,
    RuntimeConfig,
    SchemaSignature,
)
from common.progress import ProgressLogger
from .block_planner import BlockPlanner
from .line_counter import LineCounter

ProgressCallback = Optional[Callable[[FileProgress], None]]

MAX_SIGNATURE_SAMPLE_LINES = 100
ENCODING_SENTINEL_PREFIX = "ENCODING:"  # stored in header_sample for downstream phases


def detect_file_encoding(path: Path, default: str = "utf-8") -> str:
    """Very small heuristic: try utf-8, then cp1251, else fallback to default.

    Works on the first chunk of the file only to avoid IO overhead.
    """

    candidates = ["utf-8", "cp1251"]
    with path.open("rb") as handle:
        raw = handle.read(4096)
    if not raw:
        return default
    for enc in candidates:
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return default

class AdaptiveThrottle:
    """Simple moving-average based throttler that adjusts concurrency."""

    def __init__(
        self,
        *,
        max_workers: int,
        min_workers: int = 1,
        slow_threshold: float = 4.0,
        fast_threshold: float = 1.5,
        window: int = 8,
    ) -> None:
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.slow_threshold = slow_threshold
        self.fast_threshold = fast_threshold
        self.samples: deque[float] = deque(maxlen=window)
        self._limit = max_workers

    def report(self, duration: float) -> None:
        self.samples.append(duration)
        avg = sum(self.samples) / len(self.samples)
        if avg > self.slow_threshold and self._limit > self.min_workers:
            self._limit -= 1
        elif avg < self.fast_threshold and self._limit < self.max_workers:
            self._limit += 1

    @property
    def limit(self) -> int:
        return max(self.min_workers, min(self.max_workers, self._limit))

def detect_delimiter(line: str) -> str:
    candidates = [",", ";", "\t", "|"]
    counts = {c: line.count(c) for c in candidates}
    return max(counts, key=counts.get) if line else ","


def normalize_value(value: str) -> str:
    return value.strip().strip('"').strip("'")


def build_signature(block_lines: List[str], sample_cap: int, *, encoding: str) -> SchemaSignature:
    if not block_lines:
        return SchemaSignature()

    first_line = block_lines[0].rstrip("\n\r")
    delimiter = detect_delimiter(first_line)
    sample_lines = block_lines[:MAX_SIGNATURE_SAMPLE_LINES]
    header_sample = f"{ENCODING_SENTINEL_PREFIX}{encoding}" if first_line else None

    column_stats: Dict[int, ColumnStats] = {}
    col_count_counter: Counter[int] = Counter()

    for raw_line in sample_lines:
        line = raw_line.rstrip("\n\r")
        parts = line.split(delimiter)
        col_count_counter[len(parts)] += 1
        for idx, value in enumerate(parts):
            stats = column_stats.setdefault(idx, ColumnStats(index=idx))
            stats.sample_count += 1
            cleaned = normalize_value(value)
            if cleaned and len(stats.sample_values) < sample_cap:
                stats.sample_values.add(cleaned)
            update_type_flags(cleaned, stats)

    column_count = col_count_counter.most_common(1)[0][0] if col_count_counter else 0
    return SchemaSignature(
        delimiter=delimiter,
        column_count=column_count,
        header_sample=header_sample,
        columns=column_stats,
    )


def update_type_flags(value: str, stats: ColumnStats) -> None:
    if not value:
        return

    if stats.maybe_numeric:
        try:
            float(value.replace(",", "."))
        except ValueError:
            stats.maybe_numeric = False

    if stats.maybe_bool and value.lower() not in {"true", "false", "0", "1", "yes", "no"}:
        stats.maybe_bool = False

    if stats.maybe_date and not any(char in value for char in {"-", "/", "."}):
        stats.maybe_date = False


def analyze_file(
    path: Path,
    *,
    encoding: str,
    errors: str,
    block_size: int,
    min_gap_lines: int,
    sample_cap: int,
) -> FileAnalysisResult:
    line_counter = LineCounter()
    total_lines = line_counter.count(path)
    planner = BlockPlanner(block_size=block_size, min_gap_lines=min_gap_lines)
    plan = planner.plan(total_lines)
    blocks: List[FileBlock] = []

    if not plan:
        return FileAnalysisResult(file_path=path, total_lines=total_lines, blocks=[])

    for planned_block, lines in planner.iter_block_buffers(
        path,
        plan,
        encoding=encoding,
        errors=errors,
    ):
        blocks.append(
            FileBlock(
                file_path=path,
                block_id=planned_block.block_id,
                start_line=planned_block.start_line,
                end_line=planned_block.end_line,
                signature=build_signature(lines, sample_cap, encoding=encoding),
            )
        )

    return FileAnalysisResult(file_path=path, total_lines=total_lines, blocks=blocks)


def _worker_entry(args: Tuple[str, str, str, int, int, int]) -> FileAnalysisResult:
    (
        path_str,
        encoding,
        errors,
        block_size,
        min_gap_lines,
        sample_cap,
    ) = args
    return analyze_file(
        Path(path_str),
        encoding=encoding,
        errors=errors,
        block_size=block_size,
        min_gap_lines=min_gap_lines,
        sample_cap=sample_cap,
    )


class AnalysisEngine:
    """Coordinates Phase 1 analysis across multiple files."""

    def __init__(self, config: RuntimeConfig, *, progress_log: Optional[Path] = None) -> None:
        self.config = config
        self.encoding = config.global_settings.encoding
        self.errors = error_mode_from_policy(config.global_settings.error_policy)
        self.progress_logger = ProgressLogger(progress_log) if progress_log else None

    def analyze_files(
        self,
        files: Sequence[Path],
        *,
        progress_callback: ProgressCallback = None,
    ) -> List[FileAnalysisResult]:
        if not files:
            return []

        max_workers = max(1, self.config.profile.max_parallel_files)
        order_map = {path.resolve(): idx for idx, path in enumerate(files)}
        tasks = []
        for path in files:
            detected_enc = detect_file_encoding(path, default=self.encoding)
            tasks.append(
                (
                    str(path.resolve()),
                    detected_enc,
                    self.errors,
                    self.config.profile.block_size,
                    self.config.profile.min_gap_lines,
                    self.config.profile.sample_values_cap,
                )
            )

        results: List[FileAnalysisResult] = []
        if max_workers == 1:
            for task in tasks:
                start = time.perf_counter()
                result = _worker_entry(task)
                results.append(result)
                self._emit_progress(result, progress_callback)
                duration = time.perf_counter() - start
                # sequential mode ignores adaptive throttle but keeps parity in logging
            return results

        throttle = AdaptiveThrottle(max_workers=max_workers)
        task_iter = iter(tasks)
        in_flight: Dict[object, Tuple[Path, float]] = {}

        def submit_task(task_tuple: Tuple[str, str, str, int, int, int]) -> None:
            future = pool.submit(_worker_entry, task_tuple)
            in_flight[future] = (Path(task_tuple[0]), time.perf_counter())

        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            # Prime the pool respecting the current throttle limit
            while len(in_flight) < throttle.limit:
                try:
                    submit_task(next(task_iter))
                except StopIteration:
                    break

            while in_flight:
                done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    path, start_time = in_flight.pop(future)
                    result = future.result()
                    results.append(result)
                    duration = time.perf_counter() - start_time
                    throttle.report(duration)
                    self._emit_progress(result, progress_callback)
                while len(in_flight) < throttle.limit:
                    try:
                        submit_task(next(task_iter))
                    except StopIteration:
                        break
        results.sort(key=lambda item: order_map.get(item.file_path.resolve(), 0))
        return results

    def _emit_progress(
        self,
        result: FileAnalysisResult,
        progress_callback: ProgressCallback,
    ) -> None:
        progress = FileProgress(
            file_path=result.file_path,
            processed_rows=result.total_lines,
            total_rows=result.total_lines,
            current_phase="analysis-complete",
        )
        if progress_callback:
            progress_callback(progress)
        if self.progress_logger:
            self.progress_logger.emit(progress)