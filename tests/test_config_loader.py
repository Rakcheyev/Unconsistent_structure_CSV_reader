"""Tests for runtime configuration loader."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.config import error_mode_from_policy, load_runtime_config
from common.errors import BackendError, ErrorCode


def test_load_low_memory_profile_defaults() -> None:
    config = load_runtime_config("low_memory")
    assert config.profile.block_size == 1000
    assert config.profile.max_parallel_files == 1
    assert config.global_settings.encoding == "utf-8"


def test_error_mode_resolution() -> None:
    assert error_mode_from_policy("fail-fast") == "strict"
    assert error_mode_from_policy("replace") == "replace"


def test_missing_profile_raises_backend_error(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "version": 1,
            "global": {
                "encoding": "utf-8",
                "error_policy": "fail-fast",
                "synonym_dictionary": "storage/synonyms.json",
                "canonical_schema_path": "storage/canonical.json",
            },
            "profiles": {"only": _profile_payload()},
        },
    )
    with pytest.raises(BackendError) as exc:
        load_runtime_config("missing", config_path=config_path)
    assert exc.value.code == ErrorCode.CONFIG_ERROR


def test_invalid_error_policy_rejected(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "version": 1,
            "global": {
                "encoding": "utf-8",
                "error_policy": "panic",
                "synonym_dictionary": "storage/synonyms.json",
                "canonical_schema_path": "storage/canonical.json",
            },
            "profiles": {"low_memory": _profile_payload()},
        },
    )
    with pytest.raises(BackendError) as exc:
        load_runtime_config("low_memory", config_path=config_path)
    assert "error_policy" in str(exc.value)


def test_blank_path_rejected(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "version": 1,
            "global": {
                "encoding": "utf-8",
                "error_policy": "replace",
                "synonym_dictionary": " ",
                "canonical_schema_path": "storage/canonical.json",
            },
            "profiles": {"low_memory": _profile_payload()},
        },
    )
    with pytest.raises(BackendError) as exc:
        load_runtime_config("low_memory", config_path=config_path)
    assert exc.value.code == ErrorCode.CONFIG_ERROR


def _write_config(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _profile_payload() -> dict:
    return {
        "description": "tmp",
        "block_size": 1000,
        "min_gap_lines": 100,
        "max_parallel_files": 1,
        "sample_values_cap": 16,
        "writer_chunk_rows": 1000,
        "resource_limits": {
            "memory_mb": 512,
            "spill_mb": 1024,
            "max_workers": 1,
            "temp_dir": "artifacts/tmp",
        },
    }
