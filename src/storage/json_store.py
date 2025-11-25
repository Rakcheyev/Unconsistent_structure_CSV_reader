"""JSON persistence helpers for mapping configs and schema stats."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List
from uuid import UUID

from common.models import (
    ColumnProfile,
    ColumnStats,
    FileBlock,
    MappingConfig,
    SchemaDefinition,
    SchemaStats,
    SchemaColumn,
    SchemaSignature,
)


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


def mapping_to_dict(mapping: MappingConfig, *, include_samples: bool = False) -> Dict[str, object]:
    return {
        "blocks": [serialize_block(block, include_samples) for block in mapping.blocks],
        "schemas": [serialize_schema(schema) for schema in mapping.schemas],
    }


def mapping_from_dict(data: Dict[str, object]) -> MappingConfig:
    blocks_data = data.get("blocks", [])
    schemas_data = data.get("schemas", [])
    blocks = [deserialize_block(item) for item in blocks_data]
    schemas = [deserialize_schema(item) for item in schemas_data]
    return MappingConfig(blocks=blocks, schemas=schemas)


def serialize_block(block: FileBlock, include_samples: bool) -> Dict[str, object]:
    return {
        "file_path": str(block.file_path),
        "block_id": block.block_id,
        "start_line": block.start_line,
        "end_line": block.end_line,
        "schema_id": str(block.schema_id) if block.schema_id else None,
        "signature": serialize_signature(block.signature, include_samples),
    }


def deserialize_block(data: Dict[str, object]) -> FileBlock:
    signature = deserialize_signature(data.get("signature", {}))
    schema_id = data.get("schema_id")
    return FileBlock(
        file_path=Path(data["file_path"]),
        block_id=int(data["block_id"]),
        start_line=int(data["start_line"]),
        end_line=int(data["end_line"]),
        signature=signature,
        schema_id=UUID(schema_id) if schema_id else None,
    )


def serialize_signature(signature: SchemaSignature, include_samples: bool) -> Dict[str, object]:
    return {
        "delimiter": signature.delimiter,
        "column_count": signature.column_count,
        "header_sample": signature.header_sample,
        "columns": {
            str(idx): serialize_column_stats(stats, include_samples)
            for idx, stats in signature.columns.items()
        },
    }


def deserialize_signature(data: Dict[str, object]) -> SchemaSignature:
    columns_raw = data.get("columns", {})
    columns: Dict[int, ColumnStats] = {}
    for idx_str, stats in columns_raw.items():
        columns[int(idx_str)] = deserialize_column_stats(stats, index=int(idx_str))
    return SchemaSignature(
        delimiter=data.get("delimiter", ","),
        column_count=int(data.get("column_count", 0)),
        header_sample=data.get("header_sample"),
        columns=columns,
    )


def serialize_column_stats(stats: ColumnStats, include_samples: bool) -> Dict[str, object]:
    payload = {
        "sample_count": stats.sample_count,
        "maybe_numeric": stats.maybe_numeric,
        "maybe_date": stats.maybe_date,
        "maybe_bool": stats.maybe_bool,
    }
    if include_samples:
        payload["sample_values"] = sorted(stats.sample_values)
    return payload


def deserialize_column_stats(data: Dict[str, object], *, index: int = 0) -> ColumnStats:
    stats = ColumnStats(index=index)
    stats.sample_count = int(data.get("sample_count", 0))
    stats.maybe_numeric = bool(data.get("maybe_numeric", True))
    stats.maybe_date = bool(data.get("maybe_date", True))
    stats.maybe_bool = bool(data.get("maybe_bool", True))
    samples = data.get("sample_values", [])
    if samples:
        stats.sample_values.update(str(item) for item in samples)
    return stats


def serialize_schema(schema: SchemaDefinition) -> Dict[str, object]:
    return {
        "id": str(schema.id),
        "name": schema.name,
        "columns": [serialize_schema_column(col) for col in schema.columns],
    }


def deserialize_schema(data: Dict[str, object]) -> SchemaDefinition:
    columns_data = data.get("columns", [])
    return SchemaDefinition(
        id=UUID(data["id"]),
        name=str(data.get("name", "")),
        columns=[deserialize_schema_column(item) for item in columns_data],
    )


def serialize_schema_column(column: SchemaColumn) -> Dict[str, object]:
    return {
        "index": column.index,
        "raw_name": column.raw_name,
        "normalized_name": column.normalized_name,
        "data_type": column.data_type,
        "known_variants": column.known_variants,
    }


def deserialize_schema_column(data: Dict[str, object]) -> SchemaColumn:
    return SchemaColumn(
        index=int(data.get("index", 0)),
        raw_name=str(data.get("raw_name", "")),
        normalized_name=str(data.get("normalized_name", "")),
        data_type=str(data.get("data_type", "string")),
        known_variants=[str(item) for item in data.get("known_variants", [])],
    )


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
