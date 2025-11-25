"""Tests for runtime configuration loader."""
from __future__ import annotations

from common.config import error_mode_from_policy, load_runtime_config


def test_load_low_memory_profile_defaults() -> None:
    config = load_runtime_config("low_memory")
    assert config.profile.block_size == 1000
    assert config.profile.max_parallel_files == 1
    assert config.global_settings.encoding == "utf-8"


def test_error_mode_resolution() -> None:
    assert error_mode_from_policy("fail-fast") == "strict"
    assert error_mode_from_policy("replace") == "replace"
