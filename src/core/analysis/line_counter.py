"""Chunked line counting with bounded memory usage."""
from __future__ import annotations

from pathlib import Path


class LineCounter:
    """Counts newline-delimited rows without materializing the entire file."""

    def __init__(self, *, chunk_size: int = 1_048_576) -> None:
        self.chunk_size = max(1024, chunk_size)

    def count(self, path: Path) -> int:
        line_count = 0
        has_data = False
        last_char = b""
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(self.chunk_size)
                if not chunk:
                    break
                has_data = True
                line_count += chunk.count(b"\n")
                last_char = chunk[-1:]
        if has_data and last_char not in {b"\n", b""}:
            line_count += 1
        return line_count
