"""Lightweight text helpers shared across modules."""
from __future__ import annotations

import re

_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    """Return a compact, lowercase slug using alphanumeric characters only."""

    normalized = value.lower().strip()
    normalized = _SLUG_PATTERN.sub("", normalized)
    return normalized
