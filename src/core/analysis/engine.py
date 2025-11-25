"""Phase 1 analysis implementation with resource-aware limits."""
from __future__ import annotations

import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
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

ProgressCallback = Optional[Callable[[FileProgress], None]]

MAX_SIGNATURE_SAMPLE_LINES = 100
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



@dataclass(slots=True)
class PlannedBlock:
    block_id: int
    start_line: int
    end_line: int


def build_sample_indices(total_lines: int, min_gap: int) -> List[int]:
    if total_lines <= 0:
        return []

    min_gap = max(1, min_gap)
    samples = {0, max(0, total_lines - 1)}

    changed = True
    while changed:
        changed = False
        ordered = sorted(samples)
        for left, right in zip(ordered, ordered[1:]):
            if right - left > min_gap:
                mid = left + (right - left) // 2
                if mid not in samples:
                    samples.add(mid)
                    changed = True
    return sorted(samples)


def to_block(line_index: int, total_lines: int, block_size: int) -> Tuple[int, int]:
    block_size = max(1, block_size)
    total_lines = max(1, total_lines)

    half = block_size // 2
    start = max(0, line_index - half)
    end = min(total_lines - 1, start + block_size - 1)
    start = max(0, end - block_size + 1)
    return start, end


def plan_blocks(total_lines: int, block_size: int, min_gap: int) -> List[PlannedBlock]:
    indices = build_sample_indices(total_lines, min_gap)
    seen = set()
    planned: List[PlannedBlock] = []
    for block_id, idx in enumerate(indices):
        start, end = to_block(idx, total_lines, block_size)
        key = (start, end)
        if key in seen:
            continue
        seen.add(key)
        planned.append(PlannedBlock(block_id=block_id, start_line=start, end_line=end))
    return sorted(planned, key=lambda b: b.start_line)


def detect_delimiter(line: str) -> str:
    candidates = [",", ";", "\t", "|"]
    counts = {c: line.count(c) for c in candidates}
    return max(counts, key=counts.get) if line else ","


def normalize_value(value: str) -> str:
    return value.strip().strip('"').strip("'")


def build_signature(block_lines: List[str], sample_cap: int) -> SchemaSignature:
    if not block_lines:
        return SchemaSignature()

    first_line = block_lines[0].rstrip("\n\r")
    delimiter = detect_delimiter(first_line)
    sample_lines = block_lines[:MAX_SIGNATURE_SAMPLE_LINES]
    header_sample = first_line if first_line else None

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


def count_lines(path: Path, *, encoding: str, errors: str) -> int:
    line_count = 0
    with path.open("r", encoding=encoding, errors=errors) as handle:
        for _ in handle:
            line_count += 1
    return line_count


def analyze_file(
    path: Path,
    *,
    encoding: str,
    errors: str,
    block_size: int,
    min_gap_lines: int,
    sample_cap: int,
) -> FileAnalysisResult:
    total_lines = count_lines(path, encoding=encoding, errors=errors)
    plan = plan_blocks(total_lines, block_size, min_gap_lines)
    blocks: List[FileBlock] = []

    if not plan:
        return FileAnalysisResult(file_path=path, total_lines=total_lines, blocks=[])

    plan_iter = iter(plan)
    current = next(plan_iter, None)
    if current is None:
        return FileAnalysisResult(file_path=path, total_lines=total_lines, blocks=[])

    buffer: List[str] = []

    with path.open("r", encoding=encoding, errors=errors) as handle:
        for line_number, line in enumerate(handle):
            while current and line_number > current.end_line:
                blocks.append(
                    FileBlock(
                        file_path=path,
                        block_id=current.block_id,
                        start_line=current.start_line,
                        end_line=current.end_line,
                        signature=build_signature(buffer, sample_cap),
                    )
                )
                buffer.clear()
                current = next(plan_iter, None)
            if current is None:
                break
            if current.start_line <= line_number <= current.end_line:
                buffer.append(line)

        if current is not None and buffer:
            blocks.append(
                FileBlock(
                    file_path=path,
                    block_id=current.block_id,
                    start_line=current.start_line,
                    end_line=current.end_line,
                    signature=build_signature(buffer, sample_cap),
                )
            )
            buffer.clear()

        for remaining in plan_iter:
            blocks.append(
                FileBlock(
                    file_path=path,
                    block_id=remaining.block_id,
                    start_line=remaining.start_line,
                    end_line=remaining.end_line,
                    signature=build_signature([], sample_cap),
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
        tasks = [
            (
            str(path.resolve()),
                self.encoding,
                self.errors,
                self.config.profile.block_size,
                self.config.profile.min_gap_lines,
                self.config.profile.sample_values_cap,
            )
            for path in files
        ]

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