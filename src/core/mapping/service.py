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
        from difflib import SequenceMatcher
        def normalized_header_tuple(signature: SchemaSignature) -> Tuple[str, ...]:
            if not signature.header_sample:
                return tuple()
            raw_headers = [cell.strip() for cell in signature.header_sample.split(signature.delimiter)]
            return tuple(self.synonyms.normalize(h) for h in raw_headers)

        # Fuzzy grouping: merge blocks with similar normalized header tuples
        threshold = 0.85  # similarity threshold
        clusters: List[List[FileBlock]] = []
        header_tuples: List[Tuple[str, int, Tuple[str, ...]]] = []
        for block in blocks:
            sig = block.signature
            key = (sig.delimiter, sig.column_count, normalized_header_tuple(sig))
            found = False
            for i, (delim, col_count, ref_tuple) in enumerate(header_tuples):
                if delim == key[0] and col_count == key[1]:
                    # Compare header tuples by string similarity
                    s1 = "|".join(ref_tuple)
                    s2 = "|".join(key[2])
                    if SequenceMatcher(None, s1, s2).ratio() >= threshold:
                        clusters[i].append(block)
                        found = True
                        break
            if not found:
                header_tuples.append(key)
                clusters.append([block])

        schemas: List[SchemaDefinition] = []
        for block_group in clusters:
            # First block in group defines the baseline header/width and has priority.
            # Column count can grow when other files add extra columns, but must
            # never shrink below the first header's width.
            signature = block_group[0].signature
            base_schema = self._schema_from_signature(signature)
            max_columns = len(base_schema.columns)
            # Ensure we respect additional columns observed in other blocks
            # (e.g., extra trailing fields in some files).
            for block in block_group[1:]:
                sig = block.signature
                if sig.column_count and sig.column_count > max_columns:
                    max_columns = sig.column_count
            # Rebuild schema with the final column count, using the first
            # header row as the authoritative header for existing positions.
            schema = self._schema_from_signature(signature, forced_columns=max_columns)
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

    def _schema_from_signature(self, signature: SchemaSignature, forced_columns: int | None = None) -> SchemaDefinition:
        columns: List[SchemaColumn] = []
        header_values: List[str] = []
        if signature.header_sample:
            header_values = [cell.strip() for cell in signature.header_sample.split(signature.delimiter)]
        total_columns = forced_columns or signature.column_count or len(header_values) or len(signature.columns)
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