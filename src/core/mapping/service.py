"""Schema clustering and mapping helpers."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, List, Tuple

from common.models import (
    ColumnStats,
    FileBlock,
    MappingConfig,
    SchemaColumn,
    SchemaDefinition,
    SchemaSignature,
)
from core.normalization.synonyms import SynonymDictionary


@dataclass(slots=True)
class ClusterKey:
    delimiter: str
    column_count: int
    header_hash: str

    def as_tuple(self) -> Tuple[str, int, str]:
        return (self.delimiter, self.column_count, self.header_hash)


class MappingService:
    """Clusters FileBlock signatures into schema definitions."""

    def __init__(self, synonyms: SynonymDictionary | None = None) -> None:
        self.synonyms = synonyms or SynonymDictionary.empty()

    def cluster(self, blocks: List[FileBlock]) -> MappingConfig:
        grouped: Dict[Tuple[str, int, str], List[FileBlock]] = {}
        for block in blocks:
            key = self._cluster_key(block)
            grouped.setdefault(key.as_tuple(), []).append(block)

        schemas: List[SchemaDefinition] = []
        for key_tuple, block_group in grouped.items():
            signature = block_group[0].signature
            schema = self._schema_from_signature(signature)
            schemas.append(schema)
            for block in block_group:
                block.schema_id = schema.id
        schemas.sort(key=lambda s: s.name or str(s.id))
        return MappingConfig(blocks=blocks, schemas=schemas)

    def _cluster_key(self, block: FileBlock) -> ClusterKey:
        signature = block.signature
        header_text = (signature.header_sample or "").strip().lower()
        header_hash = hashlib.sha1(header_text.encode("utf-8"), usedforsecurity=False).hexdigest()
        return ClusterKey(
            delimiter=signature.delimiter,
            column_count=signature.column_count,
            header_hash=header_hash,
        )

    def _schema_from_signature(self, signature: SchemaSignature) -> SchemaDefinition:
        columns: List[SchemaColumn] = []
        header_values: List[str] = []
        if signature.header_sample:
            header_values = [cell.strip() for cell in signature.header_sample.split(signature.delimiter)]
        total_columns = signature.column_count or len(header_values) or len(signature.columns)
        for idx in range(total_columns):
            raw_name = header_values[idx] if idx < len(header_values) else f"column_{idx + 1}"
            normalized = self.synonyms.normalize(raw_name)
            stats = signature.columns.get(idx)
            columns.append(
                SchemaColumn(
                    index=idx,
                    raw_name=raw_name,
                    normalized_name=normalized,
                    data_type=infer_data_type(stats),
                    known_variants=[raw_name, normalized],
                )
            )
        name = header_values[0] if header_values else f"schema_{signature.column_count or 0}"
        return SchemaDefinition(name=name, columns=columns)


def infer_data_type(stats: ColumnStats | None) -> str:
    if not stats:
        return "string"
    if stats.maybe_bool and stats.maybe_numeric:
        return "bool"
    if stats.maybe_bool:
        return "bool"
    if stats.maybe_numeric and not stats.maybe_date:
        return "decimal"
    if stats.maybe_date and not stats.maybe_numeric:
        return "date"
    return "string"