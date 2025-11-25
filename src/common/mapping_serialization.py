"""Shared MappingConfig serialization helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List
from uuid import UUID

from .models import (
    ColumnStats,
    FileBlock,
    MappingConfig,
    SchemaDefinition,
    SchemaSignature,
    SchemaColumn,
)


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
