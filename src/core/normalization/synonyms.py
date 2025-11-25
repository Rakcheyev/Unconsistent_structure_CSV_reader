"""Synonym dictionary helpers for normalized column names."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

_CANONICALIZE_PATTERN = re.compile(r"[^a-z0-9]")


@dataclass(slots=True)
class SynonymDictionary:
    """Resolves raw column names into normalized targets via synonym mapping."""

    _lookup: Dict[str, str]

    @classmethod
    def empty(cls) -> "SynonymDictionary":
        return cls(_lookup={})

    @classmethod
    def from_file(cls, path: Path) -> "SynonymDictionary":
        if not path.exists():
            return cls.empty()
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return cls.from_mapping(data)

    @classmethod
    def from_mapping(cls, mapping: Dict[str, Iterable[str]]) -> "SynonymDictionary":
        lookup: Dict[str, str] = {}
        for canonical, variants in mapping.items():
            canonical_key = _canonicalize(canonical)
            lookup[canonical_key] = canonical
            for variant in variants:
                lookup[_canonicalize(variant)] = canonical
        return cls(_lookup=lookup)

    def normalize(self, raw_name: str) -> str:
        key = _canonicalize(raw_name)
        if not key:
            return raw_name.strip() or "column"
        return self._lookup.get(key, slugify(raw_name))

    def add_variant(self, canonical: str, variant: str) -> None:
        self._lookup[_canonicalize(variant)] = canonical


def _canonicalize(value: str) -> str:
    value = value.lower().strip()
    return _CANONICALIZE_PATTERN.sub("", value)


def slugify(value: str) -> str:
    value = value.strip().lower().replace(" ", "_")
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value or "column"
