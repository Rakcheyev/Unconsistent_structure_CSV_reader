"""Helpers for loading runtime configuration profiles."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from .models import GlobalSettings, ProfileSettings, RuntimeConfig

DEFAULT_CONFIG_PATH = Path("config/defaults.json")


def load_runtime_config(
    profile: str = "low_memory",
    *,
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> RuntimeConfig:
    """Load configuration JSON and resolve a specific profile.

    Args:
        profile: Profile name from the config file.
        config_path: Optional explicit path to the JSON file.
        overrides: Dict with optional "global" / "profile" patches.
    """

    cfg_path = config_path or DEFAULT_CONFIG_PATH
    with cfg_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    global_data = raw.get("global", {})
    profile_data = raw.get("profiles", {}).get(profile)
    if profile_data is None:
        raise ValueError(f"Profile '{profile}' not found in {cfg_path}")

    overrides = overrides or {}
    global_data = {**global_data, **overrides.get("global", {})}
    profile_data = {**profile_data, **overrides.get("profile", {})}

    global_settings = GlobalSettings(
        encoding=global_data.get("encoding", GlobalSettings().encoding),
        error_policy=global_data.get("error_policy", GlobalSettings().error_policy),
        synonym_dictionary=global_data.get(
            "synonym_dictionary", GlobalSettings().synonym_dictionary
        ),
        canonical_schema_path=global_data.get(
            "canonical_schema_path", GlobalSettings().canonical_schema_path
        ),
    )

    required_fields = (
        "description",
        "block_size",
        "min_gap_lines",
        "max_parallel_files",
        "sample_values_cap",
        "writer_chunk_rows",
    )
    missing = [field for field in required_fields if field not in profile_data]
    if missing:
        raise ValueError(f"Profile '{profile}' missing fields: {missing}")

    profile_settings = ProfileSettings(
        description=str(profile_data["description"]),
        block_size=int(profile_data["block_size"]),
        min_gap_lines=int(profile_data["min_gap_lines"]),
        max_parallel_files=max(1, int(profile_data["max_parallel_files"])),
        sample_values_cap=max(1, int(profile_data["sample_values_cap"])),
        writer_chunk_rows=int(profile_data["writer_chunk_rows"]),
    )

    return RuntimeConfig(global_settings=global_settings, profile=profile_settings)


def error_mode_from_policy(policy: str) -> str:
    """Translate human-friendly error policy into Python's encoding error handler."""

    return "strict" if policy.lower() in {"fail-fast", "strict"} else "replace"
