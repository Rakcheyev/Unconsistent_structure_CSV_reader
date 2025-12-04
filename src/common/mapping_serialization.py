"""Shared MappingConfig serialization helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List
from uuid import UUID

from .models import (
    ColumnProfileResult,
    ColumnStats,
    FileBlock,
    FileHeaderSummary,
    HeaderCluster,
    HeaderOccurrence,
    HeaderTypeProfile,
    HeaderVariant,
    MappingConfig,
    SchemaDefinition,
    SchemaSignature,
    SchemaColumn,
    SchemaMappingEntry,
)


def mapping_to_dict(mapping: MappingConfig, *, include_samples: bool = False) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "blocks": [serialize_block(block, include_samples) for block in mapping.blocks],
        "schemas": [serialize_schema(schema) for schema in mapping.schemas],
    }
    if mapping.header_clusters:
        payload["header_clusters"] = [serialize_header_cluster(cluster, include_samples) for cluster in mapping.header_clusters]
    if mapping.schema_mapping:
        payload["schema_mapping"] = [serialize_schema_mapping_entry(entry) for entry in mapping.schema_mapping]
    if mapping.file_headers:
        payload["file_headers"] = [serialize_file_header(summary) for summary in mapping.file_headers]
    if mapping.header_occurrences:
        payload["header_occurrences"] = [serialize_header_occurrence(item) for item in mapping.header_occurrences]
    if mapping.header_profiles:
        payload["header_profiles"] = [serialize_header_profile(item) for item in mapping.header_profiles]
    if mapping.column_profiles:
        payload["column_profiles"] = [serialize_column_profile_result(item) for item in mapping.column_profiles]
    return payload


def mapping_from_dict(data: Dict[str, object]) -> MappingConfig:
    blocks_data = data.get("blocks", [])
    schemas_data = data.get("schemas", [])
    blocks = [deserialize_block(item) for item in blocks_data]
    schemas = [deserialize_schema(item) for item in schemas_data]
    header_clusters_data = data.get("header_clusters", [])
    schema_mapping_data = data.get("schema_mapping", [])
    file_headers_data = data.get("file_headers", [])
    header_occurrences_data = data.get("header_occurrences", [])
    header_profiles_data = data.get("header_profiles", [])
    header_clusters = [deserialize_header_cluster(item) for item in header_clusters_data]
    schema_mapping = [deserialize_schema_mapping_entry(item) for item in schema_mapping_data]
    file_headers = [deserialize_file_header(item) for item in file_headers_data]
    header_occurrences = [deserialize_header_occurrence(item) for item in header_occurrences_data]
    header_profiles = [deserialize_header_profile(item) for item in header_profiles_data]
    column_profiles_data = data.get("column_profiles", [])
    column_profiles = [deserialize_column_profile_result(item) for item in column_profiles_data]
    return MappingConfig(
        blocks=blocks,
        schemas=schemas,
        header_clusters=header_clusters,
        schema_mapping=schema_mapping,
        file_headers=file_headers,
        header_occurrences=header_occurrences,
        header_profiles=header_profiles,
        column_profiles=column_profiles,
    )


def serialize_column_profile_result(item: ColumnProfileResult) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "file_id": item.file_id,
        "column_index": item.column_index,
        "header": item.header,
        "type_distribution": dict(item.type_distribution),
        "unique_estimate": item.unique_estimate,
        "null_count": item.null_count,
        "total_values": item.total_values,
    }
    if item.numeric_min is not None:
        payload["numeric_min"] = item.numeric_min
    if item.numeric_max is not None:
        payload["numeric_max"] = item.numeric_max
    if item.date_min is not None:
        payload["date_min"] = item.date_min
    if item.date_max is not None:
        payload["date_max"] = item.date_max
    return payload


def deserialize_column_profile_result(data: Dict[str, object]) -> ColumnProfileResult:
    return ColumnProfileResult(
        file_id=str(data.get("file_id", "")),
        column_index=int(data.get("column_index", 0)),
        header=str(data.get("header", "")),
        type_distribution={str(k): int(v) for k, v in data.get("type_distribution", {}).items()},
        unique_estimate=int(data.get("unique_estimate", 0)),
        null_count=int(data.get("null_count", 0)),
        total_values=int(data.get("total_values", 0)),
        numeric_min=float(data["numeric_min"]) if "numeric_min" in data else None,
        numeric_max=float(data["numeric_max"]) if "numeric_max" in data else None,
        date_min=str(data["date_min"]) if "date_min" in data else None,
        date_max=str(data["date_max"]) if "date_max" in data else None,
    )


def serialize_file_header(summary: FileHeaderSummary) -> Dict[str, object]:
    return {
        "file_id": summary.file_id,
        "headers": summary.headers,
    }


def deserialize_file_header(data: Dict[str, object]) -> FileHeaderSummary:
    return FileHeaderSummary(
        file_id=str(data.get("file_id", "")),
        headers=[str(item) for item in data.get("headers", [])],
    )


def serialize_header_occurrence(item: HeaderOccurrence) -> Dict[str, object]:
    return {
        "raw_header": item.raw_header,
        "file_id": item.file_id,
        "column_index": item.column_index,
    }


def deserialize_header_occurrence(data: Dict[str, object]) -> HeaderOccurrence:
    return HeaderOccurrence(
        raw_header=str(data.get("raw_header", "")),
        file_id=str(data.get("file_id", "")),
        column_index=int(data.get("column_index", 0)),
    )


def serialize_header_profile(item: HeaderTypeProfile) -> Dict[str, object]:
    return {
        "raw_header": item.raw_header,
        "type_profile": dict(item.type_profile),
    }


def deserialize_header_profile(data: Dict[str, object]) -> HeaderTypeProfile:
    profile_data = {str(k): int(v) for k, v in data.get("type_profile", {}).items()}
    return HeaderTypeProfile(
        raw_header=str(data.get("raw_header", "")),
        type_profile=profile_data,
    )


def serialize_header_variant(variant: HeaderVariant, include_samples: bool) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "file_path": str(variant.file_path),
        "column_index": variant.column_index,
        "raw_name": variant.raw_name,
        "normalized_name": variant.normalized_name,
        "detected_types": dict(variant.detected_types),
        "row_count": variant.row_count,
    }
    if include_samples and variant.sample_values:
        payload["sample_values"] = sorted(variant.sample_values)
    return payload


def deserialize_header_variant(data: Dict[str, object]) -> HeaderVariant:
    sample_values = set(str(item) for item in data.get("sample_values", []))
    return HeaderVariant(
        file_path=Path(str(data["file_path"])),
        column_index=int(data["column_index"]),
        raw_name=str(data.get("raw_name", "")),
        normalized_name=str(data.get("normalized_name", "")),
        detected_types={str(k): int(v) for k, v in data.get("detected_types", {}).items()},
        sample_values=sample_values,
        row_count=int(data.get("row_count", 0)),
    )


def serialize_header_cluster(cluster: HeaderCluster, include_samples: bool) -> Dict[str, object]:
    return {
        "cluster_id": str(cluster.cluster_id),
        "canonical_name": cluster.canonical_name,
        "variants": [serialize_header_variant(v, include_samples) for v in cluster.variants],
        "confidence_score": cluster.confidence_score,
        "needs_review": cluster.needs_review,
    }


def deserialize_header_cluster(data: Dict[str, object]) -> HeaderCluster:
    variants_data = data.get("variants", [])
    return HeaderCluster(
        cluster_id=UUID(str(data.get("cluster_id", UUID(int=0)))) if data.get("cluster_id") else UUID(int=0),
        canonical_name=str(data.get("canonical_name", "")),
        variants=[deserialize_header_variant(item) for item in variants_data],
        confidence_score=float(data.get("confidence_score", 1.0)),
        needs_review=bool(data.get("needs_review", False)),
    )


def serialize_schema_mapping_entry(entry: SchemaMappingEntry) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "file_path": str(entry.file_path),
        "source_index": entry.source_index,
        "canonical_name": entry.canonical_name,
        "target_index": entry.target_index,
    }
    if entry.offset_from_index is not None:
        payload["offset_from_index"] = entry.offset_from_index
    if entry.offset_reason is not None:
        payload["offset_reason"] = entry.offset_reason
    if entry.offset_confidence is not None:
        payload["offset_confidence"] = entry.offset_confidence
    return payload


def deserialize_schema_mapping_entry(data: Dict[str, object]) -> SchemaMappingEntry:
    return SchemaMappingEntry(
        file_path=Path(str(data["file_path"])),
        source_index=int(data["source_index"]),
        canonical_name=str(data.get("canonical_name", "")),
        target_index=int(data["target_index"]),
        offset_from_index=int(data["offset_from_index"]) if "offset_from_index" in data else None,
        offset_reason=str(data["offset_reason"]) if "offset_reason" in data else None,
        offset_confidence=float(data["offset_confidence"]) if "offset_confidence" in data else None,
    )


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
        "type_counts": dict(stats.type_counts),
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
    stats.type_counts = {str(k): int(v) for k, v in data.get("type_counts", {}).items()}
    samples = data.get("sample_values", [])
    if samples:
        stats.sample_values.update(str(item) for item in samples)
    return stats


def serialize_schema(schema: SchemaDefinition) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "id": str(schema.id),
        "name": schema.name,
        "columns": [serialize_schema_column(col) for col in schema.columns],
    }
    if schema.canonical_schema_id:
        payload["canonical_schema_id"] = schema.canonical_schema_id
    if schema.canonical_namespace:
        payload["canonical_namespace"] = schema.canonical_namespace
    return payload


def deserialize_schema(data: Dict[str, object]) -> SchemaDefinition:
    columns_data = data.get("columns", [])
    return SchemaDefinition(
        id=UUID(data["id"]),
        name=str(data.get("name", "")),
        columns=[deserialize_schema_column(item) for item in columns_data],
        canonical_schema_id=str(data["canonical_schema_id"]) if data.get("canonical_schema_id") else None,
        canonical_namespace=str(data["canonical_namespace"]) if data.get("canonical_namespace") else None,
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
