"""Shared helpers for column type classification."""
from __future__ import annotations

import re
from typing import Final

TYPE_BUCKETS: Final[tuple[str, ...]] = ("date", "integer", "float", "text", "empty")

_DATE_PATTERN = re.compile(r"\b\d{1,4}[./-]\d{1,2}[./-]\d{1,4}\b")
_INT_PATTERN = re.compile(r"^[+-]?\d+$")
_FLOAT_PATTERN = re.compile(r"^[+-]?(?:\d+[.,]\d+|\d+\.\d*|\d*[.,]\d+)$")


def classify_value(value: str) -> str:
    """Classify a string into a coarse column type bucket."""

    if value is None:
        return "empty"
    cleaned = value.strip()
    if not cleaned:
        return "empty"
    if _DATE_PATTERN.search(cleaned):
        return "date"
    if _INT_PATTERN.fullmatch(cleaned):
        return "integer"
    normalized = cleaned.replace(",", ".")
    if _FLOAT_PATTERN.fullmatch(normalized):
        # Values that matched the integer pattern already returned "integer".
        return "float"
    return "text"


def ensure_type_buckets(counts: dict[str, int]) -> dict[str, int]:
    """Ensure all standard buckets are present in the result dict."""

    return {bucket: counts.get(bucket, 0) for bucket in TYPE_BUCKETS}
