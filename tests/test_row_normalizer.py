from __future__ import annotations

from pathlib import Path

from common.models import SchemaColumn, SchemaDefinition, SchemaMappingEntry
from core.materialization.runner import RowNormalizer


def _build_schema() -> SchemaDefinition:
    return SchemaDefinition(
        name="customers",
        columns=[
            SchemaColumn(index=0, raw_name="name", normalized_name="name"),
            SchemaColumn(index=1, raw_name="email", normalized_name="email"),
            SchemaColumn(index=2, raw_name="age", normalized_name="age"),
        ],
    )


def test_row_normalizer_reorders_columns(tmp_path: Path) -> None:
    file_path = tmp_path / "customers.csv"
    mappings = [
        SchemaMappingEntry(
            file_path=file_path,
            source_index=0,
            canonical_name="email",
            target_index=1,
        ),
        SchemaMappingEntry(
            file_path=file_path,
            source_index=1,
            canonical_name="name",
            target_index=0,
        ),
    ]
    normalizer = RowNormalizer(mappings)
    schema = _build_schema()
    normalized = normalizer.normalize(
        ["alice@example.com", "Alice", "30"],
        schema,
        source_path=file_path,
    )
    assert normalized.values == ["Alice", "alice@example.com", "30"]
    assert normalized.observed_length == 3


def test_row_normalizer_fills_missing_columns(tmp_path: Path) -> None:
    file_path = tmp_path / "customers.csv"
    normalizer = RowNormalizer([])
    schema = _build_schema()
    normalized = normalizer.normalize(["Alice", "alice@example.com"], schema, source_path=file_path)
    assert normalized.values == ["Alice", "alice@example.com"]
    assert normalized.observed_length == 2


def test_row_normalizer_handles_per_file_mappings(tmp_path: Path) -> None:
    file_a = tmp_path / "customers_a.csv"
    file_b = tmp_path / "customers_b.csv"
    mappings = [
        SchemaMappingEntry(file_path=file_a, source_index=0, canonical_name="name", target_index=0),
        SchemaMappingEntry(file_path=file_a, source_index=1, canonical_name="email", target_index=1),
        SchemaMappingEntry(file_path=file_b, source_index=0, canonical_name="email", target_index=1),
        SchemaMappingEntry(file_path=file_b, source_index=1, canonical_name="name", target_index=0),
    ]
    normalizer = RowNormalizer(mappings)
    schema = _build_schema()

    normalized_a = normalizer.normalize(["Alice", "alice@example.com", "30"], schema, source_path=file_a)
    normalized_b = normalizer.normalize(["bob@example.com", "Bob", "27"], schema, source_path=file_b)

    assert normalized_a.values[:2] == ["Alice", "alice@example.com"]
    assert normalized_b.values[:2] == ["Bob", "bob@example.com"]


def test_row_normalizer_uses_canonical_lookup(tmp_path: Path) -> None:
    file_path = tmp_path / "customers_alt.csv"
    mappings = [
        SchemaMappingEntry(
            file_path=file_path,
            source_index=0,
            canonical_name="Age",
            target_index=None,
        )
    ]
    normalizer = RowNormalizer(mappings)
    schema = _build_schema()
    normalized = normalizer.normalize(["42", ""], schema, source_path=file_path)
    assert normalized.values[2] == "42"
    assert normalized.observed_length == 2
