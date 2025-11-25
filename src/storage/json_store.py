"""JSON persistence helpers for mapping configs and schema stats."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List
from uuid import UUID

from common.mapping_serialization import mapping_from_dict, mapping_to_dict
from common.models import ColumnProfile, MappingConfig, SchemaStats


def save_mapping_config(
    mapping: MappingConfig,
    path: Path,
    *,
    include_samples: bool = False,
) -> None:
    """Serialize mapping config to JSON with optional sample values."""

    data = mapping_to_dict(mapping, include_samples=include_samples)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_mapping_config(path: Path) -> MappingConfig:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return mapping_from_dict(data)




def save_schema_stats(stats: Iterable[SchemaStats], path: Path) -> None:
    data = [serialize_schema_stats(item) for item in stats]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_schema_stats(path: Path) -> List[SchemaStats]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return [deserialize_schema_stats(item) for item in data]


def serialize_schema_stats(stats: SchemaStats) -> Dict[str, object]:
    return {
        "schema_id": str(stats.schema_id),
        "row_count": stats.row_count,
        "columns": [serialize_column_profile(col) for col in stats.columns],
    }


def deserialize_schema_stats(data: Dict[str, object]) -> SchemaStats:
    return SchemaStats(
        schema_id=UUID(data["schema_id"]),
        row_count=int(data.get("row_count", 0)),
        columns=[deserialize_column_profile(item) for item in data.get("columns", [])],
    )


def serialize_column_profile(profile: ColumnProfile) -> Dict[str, object]:
    return {
        "name": profile.name,
        "unique_count_estimate": profile.unique_count_estimate,
        "top_values": profile.top_values,
    }


def deserialize_column_profile(data: Dict[str, object]) -> ColumnProfile:
    return ColumnProfile(
        name=str(data.get("name", "")),
        unique_count_estimate=data.get("unique_count_estimate"),
        top_values=[str(item) for item in data.get("top_values", [])],
    )
