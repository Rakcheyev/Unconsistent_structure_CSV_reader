from __future__ import annotations

from pathlib import Path

from common.models import ColumnProfileResult, SchemaColumn, SchemaDefinition, SchemaMappingEntry
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


def test_row_normalizer_uses_type_profiles_for_unknown_headers(tmp_path: Path) -> None:
    file_path = tmp_path / "financials.csv"
    mappings = [
        SchemaMappingEntry(
            file_path=file_path,
            source_index=0,
            canonical_name="gross_amount",
            target_index=None,
        )
    ]
    profiles = [
        ColumnProfileResult(
            file_id=file_path.as_posix(),
            column_index=0,
            header="gross_amount",
            type_distribution={"float": 4, "null": 0},
            unique_estimate=4,
            null_count=0,
            total_values=4,
            numeric_min=100.0,
            numeric_max=150.0,
        )
    ]
    schema = SchemaDefinition(
        name="financials",
        columns=[
            SchemaColumn(index=0, raw_name="id", normalized_name="id", data_type="string"),
            SchemaColumn(index=1, raw_name="amount", normalized_name="amount", data_type="decimal"),
        ],
    )
    normalizer = RowNormalizer(mappings, column_profiles=profiles)
    normalized = normalizer.normalize(["123.45", "n/a"], schema, source_path=file_path)
    assert normalized.values[1] == "123.45"
