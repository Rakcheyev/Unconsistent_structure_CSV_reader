"""Helpers for loading runtime configuration profiles."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .errors import BackendError, ErrorCode
from .models import GlobalSettings, ProfileSettings, ResourceLimits, RuntimeConfig

DEFAULT_CONFIG_PATH = Path("config/defaults.json")
ALLOWED_ERROR_POLICIES = {"fail-fast", "strict", "replace"}


@dataclass(slots=True)
class ConfigDocument:
    source: Path
    version: int
    global_settings: GlobalSettings
    profiles: Dict[str, ProfileSettings]


def load_runtime_config(
    profile: str = "low_memory",
    *,
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> RuntimeConfig:
    """Load configuration JSON, validate it, and resolve a specific profile."""

    document = load_config_document(
        profile_name=profile,
        config_path=config_path,
        overrides=overrides,
    )
    try:
        profile_settings = document.profiles[profile]
    except KeyError as exc:  # pragma: no cover - guarded earlier but defensive
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"Profile '{profile}' not found in {document.source}",
        ) from exc
    return RuntimeConfig(global_settings=document.global_settings, profile=profile_settings)


def load_config_document(
    *,
    profile_name: Optional[str] = None,
    config_path: Optional[Path] = None,
    overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> ConfigDocument:
    cfg_path = config_path or DEFAULT_CONFIG_PATH
    raw = _read_config_json(cfg_path)

    version = _require_positive_int(raw.get("version"), "version", cfg_path)
    global_section = raw.get("global")
    if not isinstance(global_section, Mapping):
        raise BackendError(ErrorCode.CONFIG_ERROR, f"'global' section missing in {cfg_path}")

    overrides = overrides or {}
    global_data = {**global_section, **(overrides.get("global") or {})}
    global_settings = _build_global_settings(global_data, cfg_path)

    profiles_section = raw.get("profiles")
    if not isinstance(profiles_section, Mapping) or not profiles_section:
        raise BackendError(ErrorCode.CONFIG_ERROR, f"'profiles' section missing in {cfg_path}")

    profile_overrides = overrides.get("profile") or {}
    profiles: Dict[str, ProfileSettings] = {}
    for name, profile_data in profiles_section.items():
        if not isinstance(profile_data, Mapping):
            raise BackendError(
                ErrorCode.CONFIG_ERROR,
                f"Profile '{name}' must be an object in {cfg_path}",
            )
        merged = dict(profile_data)
        if profile_name and name == profile_name and profile_overrides:
            merged = {**merged, **profile_overrides}
        profiles[name] = _build_profile_settings(name, merged, cfg_path)

    if profile_name and profile_name not in profiles:
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"Profile '{profile_name}' not found in {cfg_path}",
        )

    return ConfigDocument(
        source=cfg_path,
        version=version,
        global_settings=global_settings,
        profiles=profiles,
    )


def error_mode_from_policy(policy: str) -> str:
    """Translate human-friendly error policy into Python's encoding error handler."""

    return "strict" if policy.lower() in {"fail-fast", "strict"} else "replace"


# ---------------------------------------------------------------------------
# Internal helpers


def _read_config_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:  # pragma: no cover - depends on filesystem
        raise BackendError(ErrorCode.CONFIG_ERROR, f"Config file '{path}' not found") from exc
    except json.JSONDecodeError as exc:
        raise BackendError(ErrorCode.CONFIG_ERROR, f"Config file '{path}' is not valid JSON: {exc}") from exc


def _build_global_settings(data: Mapping[str, Any], source: Path) -> GlobalSettings:
    encoding = _require_string(data.get("encoding", GlobalSettings().encoding), "global.encoding", source)
    error_policy = _normalize_error_policy(
        data.get("error_policy", GlobalSettings().error_policy),
        source,
    )
    synonym_dictionary = _require_path(
        data.get("synonym_dictionary", GlobalSettings().synonym_dictionary),
        "global.synonym_dictionary",
        source,
    )
    canonical_schema_path = _require_path(
        data.get("canonical_schema_path", GlobalSettings().canonical_schema_path),
        "global.canonical_schema_path",
        source,
    )
    return GlobalSettings(
        encoding=encoding,
        error_policy=error_policy,
        synonym_dictionary=synonym_dictionary,
        canonical_schema_path=canonical_schema_path,
    )


def _build_profile_settings(name: str, data: Mapping[str, Any], source: Path) -> ProfileSettings:
    prefix = f"profiles.{name}"
    required_fields = (
        "description",
        "block_size",
        "min_gap_lines",
        "max_parallel_files",
        "sample_values_cap",
        "writer_chunk_rows",
    )
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"Profile '{name}' missing fields {missing} in {source}",
        )

    description = _require_string(data.get("description"), f"{prefix}.description", source)
    block_size = _require_positive_int(data.get("block_size"), f"{prefix}.block_size", source)
    min_gap_lines = _require_positive_int(data.get("min_gap_lines"), f"{prefix}.min_gap_lines", source)
    max_parallel_files = _require_positive_int(
        data.get("max_parallel_files"), f"{prefix}.max_parallel_files", source
    )
    sample_values_cap = _require_positive_int(
        data.get("sample_values_cap"), f"{prefix}.sample_values_cap", source
    )
    writer_chunk_rows = _require_positive_int(
        data.get("writer_chunk_rows"), f"{prefix}.writer_chunk_rows", source
    )

    limits_data = data.get("resource_limits", {}) or {}
    if not isinstance(limits_data, Mapping):
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"{prefix}.resource_limits must be an object in {source}",
        )
    resource_limits = ResourceLimits(
        memory_mb=_optional_positive_int(
            limits_data.get("memory_mb"), f"{prefix}.resource_limits.memory_mb", source
        ),
        spill_mb=_optional_positive_int(
            limits_data.get("spill_mb"), f"{prefix}.resource_limits.spill_mb", source
        ),
        max_workers=_optional_positive_int(
            limits_data.get("max_workers"), f"{prefix}.resource_limits.max_workers", source
        ),
        temp_dir=_require_path(
            limits_data.get("temp_dir", ResourceLimits().temp_dir),
            f"{prefix}.resource_limits.temp_dir",
            source,
        ),
    )

    return ProfileSettings(
        description=description,
        block_size=block_size,
        min_gap_lines=min_gap_lines,
        max_parallel_files=max_parallel_files,
        sample_values_cap=sample_values_cap,
        writer_chunk_rows=writer_chunk_rows,
        resource_limits=resource_limits,
    )


def _normalize_error_policy(value: Any, source: Path) -> str:
    policy = _require_string(value, "global.error_policy", source).lower()
    if policy not in {p.lower() for p in ALLOWED_ERROR_POLICIES}:
        allowed = ", ".join(sorted(ALLOWED_ERROR_POLICIES))
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"Unsupported error_policy '{value}' in {source}. Allowed: {allowed}",
        )
    return "fail-fast" if policy in {"fail-fast", "strict"} else "replace"


def _require_string(value: Any, field: str, source: Path) -> str:
    if not isinstance(value, str):
        raise BackendError(ErrorCode.CONFIG_ERROR, f"{field} must be a string in {source}")
    text = value.strip()
    if not text:
        raise BackendError(ErrorCode.CONFIG_ERROR, f"{field} must be non-empty in {source}")
    return text


def _require_path(value: Any, field: str, source: Path) -> str:
    path = _require_string(value, field, source)
    return path


def _require_positive_int(value: Any, field: str, source: Path) -> int:
    try:
        num = int(value)
    except (TypeError, ValueError) as exc:
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"{field} must be an integer in {source}",
        ) from exc
    if num <= 0:
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"{field} must be greater than zero in {source}",
        )
    return num


def _optional_positive_int(value: Any, field: str, source: Path) -> Optional[int]:
    if value is None:
        return None
    try:
        num = int(value)
    except (TypeError, ValueError) as exc:
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"{field} must be an integer in {source}",
        ) from exc
    if num <= 0:
        raise BackendError(
            ErrorCode.CONFIG_ERROR,
            f"{field} must be greater than zero in {source}",
        )
    return num
