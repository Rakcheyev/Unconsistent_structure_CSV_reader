"""Planning and buffered block extraction for sampling."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Sequence, Tuple

BUFFER_LIMIT_BYTES = 1_048_576


@dataclass(slots=True)
class PlannedBlock:
    block_id: int
    start_line: int
    end_line: int


class BlockPlanner:
    """Builds sampling plans and streams block buffers within a memory cap."""

    def __init__(
        self,
        *,
        block_size: int,
        min_gap_lines: int,
        buffer_limit_bytes: int = BUFFER_LIMIT_BYTES,
    ) -> None:
        self.block_size = max(1, block_size)
        self.min_gap_lines = max(1, min_gap_lines)
        self.buffer_limit_bytes = max(1, buffer_limit_bytes)

    def plan(self, total_lines: int) -> List[PlannedBlock]:
        indices = self._build_sample_indices(total_lines)
        seen: set[Tuple[int, int]] = set()
        planned: List[PlannedBlock] = []
        for block_id, idx in enumerate(indices):
            start, end = self._to_block(idx, total_lines)
            key = (start, end)
            if key in seen:
                continue
            seen.add(key)
            planned.append(PlannedBlock(block_id=block_id, start_line=start, end_line=end))
        planned.sort(key=lambda block: block.start_line)
        return planned

    def iter_block_buffers(
        self,
        path: Path,
        plan: Sequence[PlannedBlock],
        *,
        encoding: str,
        errors: str,
    ) -> Iterator[Tuple[PlannedBlock, List[str]]]:
        if not plan:
            return

        plan_iter = iter(plan)
        current = next(plan_iter, None)
        if current is None:
            return

        buffer: List[str] = []
        buffer_bytes = 0

        def flush_buffer(block: PlannedBlock) -> Iterator[Tuple[PlannedBlock, List[str]]]:
            nonlocal buffer, buffer_bytes
            captured = buffer.copy()
            buffer.clear()
            buffer_bytes = 0
            yield block, captured

        with path.open("rb") as handle:
            for line_number, raw_line in enumerate(handle):
                while current and line_number > current.end_line:
                    yield from flush_buffer(current)
                    current = next(plan_iter, None)
                if current is None:
                    break
                if current.start_line <= line_number <= current.end_line:
                    if buffer_bytes + len(raw_line) <= self.buffer_limit_bytes:
                        buffer.append(raw_line.decode(encoding, errors=errors))
                        buffer_bytes += len(raw_line)
                if current is not None and line_number == current.end_line:
                    yield from flush_buffer(current)
                    current = next(plan_iter, None)
            if current is not None:
                yield from flush_buffer(current)
                current = next(plan_iter, None)
        for remaining in plan_iter:
            yield remaining, []

    def _build_sample_indices(self, total_lines: int) -> List[int]:
        if total_lines <= 0:
            return []
        gap = max(1, self.min_gap_lines)
        samples: set[int] = {0, max(0, total_lines - 1)}
        changed = True
        while changed:
            changed = False
            ordered = sorted(samples)
            for left, right in zip(ordered, ordered[1:]):
                if right - left > gap:
                    mid = left + (right - left) // 2
                    if mid not in samples:
                        samples.add(mid)
                        changed = True
        return sorted(samples)

    def _to_block(self, line_index: int, total_lines: int) -> Tuple[int, int]:
        half = self.block_size // 2
        total_lines = max(1, total_lines)
        start = max(0, line_index - half)
        end = min(total_lines - 1, start + self.block_size - 1)
        start = max(0, end - self.block_size + 1)
        return start, end
