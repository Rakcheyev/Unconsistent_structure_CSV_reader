"""Normalization utilities for applying synonym dictionaries."""
from __future__ import annotations

from common.models import (
    CanonicalSchemaRegistry,
    MappingConfig,
    SchemaColumn,
    SchemaDefinition,
)
from common.text import slugify
from core.validation.canonical import resolve_canonical_schema

from .synonyms import SynonymDictionary


class NormalizationService:
    """Updates schema columns with normalized names and variant tracking."""

    def __init__(
        self,
        synonyms: SynonymDictionary | None = None,
        *,
        canonical_registry: CanonicalSchemaRegistry | None = None,
    ) -> None:
        self.synonyms = synonyms or SynonymDictionary.empty()
        self.canonical_registry = canonical_registry

    def apply(self, mapping: MappingConfig) -> MappingConfig:
        for schema in mapping.schemas:
            for column in schema.columns:
                self._apply_to_column(column)
            self._apply_canonical_contract(schema)
        return mapping

    def _apply_to_column(self, column: SchemaColumn) -> None:
        raw = column.raw_name or column.normalized_name or f"column_{column.index + 1}"
        normalized = self.synonyms.normalize(raw)
        column.normalized_name = normalized
        if raw and raw not in column.known_variants:
            column.known_variants.append(raw)
        if normalized not in column.known_variants:
            column.known_variants.append(normalized)

    def _apply_canonical_contract(self, schema: SchemaDefinition) -> None:
        if not self.canonical_registry:
            return
        canonical = resolve_canonical_schema(schema, self.canonical_registry)
        if not canonical:
            return
        schema.canonical_schema_id = canonical.schema_id
        schema.canonical_namespace = canonical.namespace
        column_by_slug = {
            slugify(column.normalized_name or column.raw_name or f"column_{column.index + 1}"): column
            for column in schema.columns
        }
        for spec in canonical.columns:
            slug = slugify(spec.name)
            column = column_by_slug.get(slug)
            if column:
                column.data_type = spec.data_type
