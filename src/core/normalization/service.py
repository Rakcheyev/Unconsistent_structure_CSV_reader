"""Normalization utilities for applying synonym dictionaries."""
from __future__ import annotations

from common.models import MappingConfig, SchemaColumn

from .synonyms import SynonymDictionary


class NormalizationService:
    """Updates schema columns with normalized names and variant tracking."""

    def __init__(self, synonyms: SynonymDictionary | None = None) -> None:
        self.synonyms = synonyms or SynonymDictionary.empty()

    def apply(self, mapping: MappingConfig) -> MappingConfig:
        for schema in mapping.schemas:
            for column in schema.columns:
                self._apply_to_column(column)
        return mapping

    def _apply_to_column(self, column: SchemaColumn) -> None:
        raw = column.raw_name or column.normalized_name or f"column_{column.index + 1}"
        normalized = self.synonyms.normalize(raw)
        column.normalized_name = normalized
        if raw and raw not in column.known_variants:
            column.known_variants.append(raw)
        if normalized not in column.known_variants:
            column.known_variants.append(normalized)
