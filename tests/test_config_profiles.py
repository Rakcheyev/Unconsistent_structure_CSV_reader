"""Smoke tests for default configuration profiles."""
from __future__ import annotations

import json
from pathlib import Path


REQUIRED_FIELDS = {"block_size", "min_gap_lines", "max_parallel_files", "sample_values_cap", "writer_chunk_rows"}


def load_defaults() -> dict:
    root = Path(__file__).resolve().parents[1]
    with (root / "config" / "defaults.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def test_profiles_present() -> None:
    defaults = load_defaults()
    profiles = defaults.get("profiles", {})
    assert {"low_memory", "workstation"}.issubset(profiles), "Missing expected profiles"


def test_profile_fields_complete() -> None:
    defaults = load_defaults()
    for name, profile in defaults.get("profiles", {}).items():
        missing = REQUIRED_FIELDS - set(profile)
        assert not missing, f"Profile '{name}' missing fields: {sorted(missing)}"
