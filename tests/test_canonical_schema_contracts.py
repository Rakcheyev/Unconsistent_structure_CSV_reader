import json
from pathlib import Path

from common.models import (
    CanonicalSchema,
    CanonicalSchemaRegistry,
    MappingConfig,
    SchemaColumn,
    SchemaDefinition,
)
from core.normalization import NormalizationService, SynonymDictionary
from core.validation.canonical import load_canonical_registry


FIXTURE_PATH = Path("storage/canonical_schemas.json")


def load_fixture_payload() -> dict:
    with FIXTURE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def test_load_canonical_registry_from_fixture() -> None:
    registry = load_canonical_registry(FIXTURE_PATH)
    assert registry.get("retail_orders", namespace="retail") is not None
    assert registry.get("store_inventory", namespace="retail") is not None


def test_canonical_schema_round_trip_serialization() -> None:
    payload = load_fixture_payload()["schemas"][0]
    schema = CanonicalSchema.from_dict(payload)
    round_trip = CanonicalSchema.from_dict(schema.to_dict())

    assert schema.schema_id == round_trip.schema_id
    assert schema.column_names() == round_trip.column_names()
    assert schema.required_columns() == round_trip.required_columns()


def test_normalization_service_attaches_canonical_contract() -> None:
    registry = load_canonical_registry(FIXTURE_PATH)
    schema = SchemaDefinition(
        name="retail_orders",
        columns=[
            SchemaColumn(index=0, raw_name="order_id", normalized_name="order_id"),
            SchemaColumn(index=1, raw_name="gross_total", normalized_name="gross_total"),
        ],
    )
    mapping = MappingConfig(blocks=[], schemas=[schema])

    NormalizationService(
        SynonymDictionary.empty(),
        canonical_registry=registry,
    ).apply(mapping)

    assert mapping.schemas[0].canonical_schema_id == "retail_orders"
    assert mapping.schemas[0].canonical_namespace == "retail"
    assert mapping.schemas[0].columns[1].data_type == "decimal"
