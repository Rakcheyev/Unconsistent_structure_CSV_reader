"""Streaming column profiler reused across Phase 1 and review workflows."""
from __future__ import annotations

import csv
import hashlib
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from common.models import ColumnProfileResult
from core.headers.type_inference import classify_value

# Supported date patterns; kept small to avoid heavy dependencies.
_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d.%m.%Y",
    "%d-%m-%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
)


def profile_file_columns(
    path: Path,
    *,
    delimiter: str,
    encoding: str,
    errors: str,
) -> List[ColumnProfileResult]:
    """Profile each column in the given file without loading entire content into memory."""

    profiler = _ColumnProfiler(delimiter=delimiter)
    try:
        with path.open("r", encoding=encoding, errors=errors, newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            for row in reader:
                if not row:
                    continue
                if profiler.consume_header_if_needed(row):
                    continue
                profiler.observe_row(row)
    except OSError:
        return []
    return profiler.finalize(file_id=path.as_posix())


class HyperLogLogLite:
    """Approximate distinct counter inspired by HLL but tuned for tiny payloads."""

    def __init__(self, precision: int = 10) -> None:
        self.precision = min(16, max(4, precision))
        self.register_count = 1 << self.precision
        self.registers = [0] * self.register_count

    def add(self, value: str) -> None:
        if not value:
            return
        digest = hashlib.blake2b(
            value.encode("utf-8"), digest_size=8, usedforsecurity=False
        ).digest()
        hashed = int.from_bytes(digest, "big", signed=False)
        index = hashed & (self.register_count - 1)
        w = hashed >> self.precision
        leading = self._rho(w, 64 - self.precision)
        if leading > self.registers[index]:
            self.registers[index] = leading

    def estimate(self) -> int:
        m = float(self.register_count)
        alpha = 0.7213 / (1 + 1.079 / m)
        indicator = sum(2.0 ** (-register) for register in self.registers)
        if indicator == 0:
            return 0
        raw = alpha * (m * m) / indicator
        # Small-range correction (linear counting) improves precision for <= 2.5m
        zero_registers = self.registers.count(0)
        if zero_registers and raw < 2.5 * m:
            return int(m * math.log(m / zero_registers))
        return int(raw)

    @staticmethod
    def _rho(value: int, bits: int) -> int:
        if value == 0:
            return bits + 1
        leading = 1
        while leading <= bits and ((value >> (bits - leading)) & 1) == 0:
            leading += 1
        return leading


@dataclass(slots=True)
class ColumnProfileMetrics:
    index: int
    header: str
    type_distribution: Dict[str, int] = field(default_factory=lambda: {
        "integer": 0,
        "float": 0,
        "text": 0,
        "date": 0,
        "null": 0,
    })
    total_values: int = 0
    null_count: int = 0
    numeric_min: Optional[float] = None
    numeric_max: Optional[float] = None
    date_min: Optional[str] = None
    date_max: Optional[str] = None
    distinct_counter: HyperLogLogLite = field(default_factory=HyperLogLogLite)

    def observe(self, raw_value: str) -> None:
        value = raw_value.strip()
        bucket = classify_value(value)
        mapped_bucket = _map_bucket(bucket)
        self.type_distribution[mapped_bucket] = self.type_distribution.get(mapped_bucket, 0) + 1
        self.total_values += 1
        if mapped_bucket == "null":
            self.null_count += 1
            return
        self.distinct_counter.add(value)
        if mapped_bucket in {"integer", "float"}:
            maybe = _to_float(value)
            if maybe is not None:
                if self.numeric_min is None or maybe < self.numeric_min:
                    self.numeric_min = maybe
                if self.numeric_max is None or maybe > self.numeric_max:
                    self.numeric_max = maybe
        elif mapped_bucket == "date":
            iso_date = _to_iso_date(value)
            if iso_date:
                if self.date_min is None or iso_date < self.date_min:
                    self.date_min = iso_date
                if self.date_max is None or iso_date > self.date_max:
                    self.date_max = iso_date

    def to_result(self, file_id: str) -> ColumnProfileResult:
        return ColumnProfileResult(
            file_id=file_id,
            column_index=self.index,
            header=self.header,
            type_distribution=dict(self.type_distribution),
            unique_estimate=self.distinct_counter.estimate(),
            null_count=self.null_count,
            total_values=self.total_values,
            numeric_min=self.numeric_min,
            numeric_max=self.numeric_max,
            date_min=self.date_min,
            date_max=self.date_max,
        )


class _ColumnProfiler:
    def __init__(self, *, delimiter: str) -> None:
        self.delimiter = delimiter or ","
        self.headers: List[str] = []
        self.metrics: Dict[int, ColumnProfileMetrics] = {}

    def consume_header_if_needed(self, row: List[str]) -> bool:
        if self.headers:
            return False
        normalized = [cell.strip() for cell in row]
        self.headers = [value or f"column_{idx + 1}" for idx, value in enumerate(normalized)]
        return True

    def observe_row(self, row: List[str]) -> None:
        if not self.headers:
            self.consume_header_if_needed(row)
            return
        width = max(len(row), len(self.headers))
        while len(self.headers) < width:
            self.headers.append(f"column_{len(self.headers) + 1}")
        for idx in range(width):
            value = row[idx] if idx < len(row) else ""
            metric = self.metrics.get(idx)
            if metric is None:
                metric = ColumnProfileMetrics(index=idx, header=self.headers[idx])
                self.metrics[idx] = metric
            metric.observe(value)

    def finalize(self, *, file_id: str) -> List[ColumnProfileResult]:
        results = []
        for idx in range(len(self.headers)):
            metric = self.metrics.get(idx)
            if metric is None:
                metric = ColumnProfileMetrics(index=idx, header=self.headers[idx])
            results.append(metric.to_result(file_id))
        results.sort(key=lambda item: item.column_index)
        return results


def _map_bucket(bucket: str) -> str:
    if bucket == "empty":
        return "null"
    if bucket in {"integer", "float", "text", "date"}:
        return bucket
    return "text"


def _to_float(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        normalized = value.replace(" ", "").replace(",", ".")
        return float(normalized)
    except ValueError:
        return None


def _to_iso_date(value: str) -> Optional[str]:
    if not value:
        return None
    # Fast-path: ISO8601 parsing with datetime.fromisoformat
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.date().isoformat()
    except ValueError:
        pass
    for pattern in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(value, pattern)
            return parsed.date().isoformat()
        except ValueError:
            continue
    return None
