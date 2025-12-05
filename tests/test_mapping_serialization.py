from __future__ import annotations

from pathlib import Path

from common.models import (
    ColumnProfileResult,
    ColumnStats,
    FileBlock,
    HeaderCluster,
    HeaderVariant,
    MappingConfig,
    SchemaColumn,
    SchemaDefinition,
    SchemaSignature,
    SchemaMappingEntry,
)
from common.versioning import HEADER_CLUSTER_VERSION, MAPPING_ARTIFACT_VERSION


def _build_mapping() -> MappingConfig:
    stats = ColumnStats(index=0)
    stats.sample_count = 2
    stats.sample_values.update({"foo", "bar"})
    block = FileBlock(
        file_path=Path("input.csv"),
        block_id=1,
        start_line=0,
        end_line=4,
        signature=SchemaSignature(columns={0: stats}),
    )
    schema = SchemaDefinition(columns=[SchemaColumn(index=0, raw_name="col0")])

    header_variant = HeaderVariant(
        file_path=Path("input.csv"),
        column_index=0,
        raw_name="col0",
        normalized_name="col0_norm",
        detected_types={"str": 2},
        sample_values={"foo", "bar"},
        row_count=2,
    )
    header_cluster = HeaderCluster(
        canonical_name="col0_norm",
        variants=[header_variant],
    )

    mapping_entry = SchemaMappingEntry(
        file_path=Path("input.csv"),
        source_index=0,
        canonical_name="col0_norm",
        target_index=0,
    )

    column_profile = ColumnProfileResult(
        file_id="input.csv",
        column_index=0,
        header="col0",
        type_distribution={"text": 2, "null": 0},
        unique_estimate=2,
        null_count=0,
        total_values=2,
    )

    return MappingConfig(
        blocks=[block],
        schemas=[schema],
        header_clusters=[header_cluster],
        schema_mapping=[mapping_entry],
        column_profiles=[column_profile],
    )


def test_mapping_config_to_dict_excludes_samples_by_default():
    payload = _build_mapping().to_dict()
    assert payload["artifact_version"] == MAPPING_ARTIFACT_VERSION
    column_payload = payload["blocks"][0]["signature"]["columns"]["0"]
    assert "sample_values" not in column_payload

    # HeaderVariant sample values should also be excluded by default
    cluster_payload = payload["header_clusters"][0]
    variant_payload = cluster_payload["variants"][0]
    assert "sample_values" not in variant_payload
    assert cluster_payload["version"] == HEADER_CLUSTER_VERSION

    assert payload["column_profiles"][0]["header"] == "col0"


def test_mapping_config_round_trip_with_samples():
    mapping = _build_mapping()
    payload = mapping.to_dict(include_samples=True)
    column_payload = payload["blocks"][0]["signature"]["columns"]["0"]
    assert sorted(column_payload["sample_values"]) == ["bar", "foo"]

    # HeaderVariant sample values should be present when include_samples=True
    cluster_payload = payload["header_clusters"][0]
    variant_payload = cluster_payload["variants"][0]
    assert sorted(variant_payload["sample_values"]) == ["bar", "foo"]

    restored = MappingConfig.from_dict(payload)
    restored_stats = restored.blocks[0].signature.columns[0]
    assert restored_stats.sample_values == {"bar", "foo"}
    assert restored.schemas[0].columns[0].raw_name == "col0"

    # Header clusters and schema mapping should also round-trip
    assert len(restored.header_clusters) == 1
    restored_cluster = restored.header_clusters[0]
    assert restored_cluster.canonical_name == "col0_norm"
    assert len(restored_cluster.variants) == 1
    restored_variant = restored_cluster.variants[0]
    assert restored_variant.raw_name == "col0"
    assert restored_variant.normalized_name == "col0_norm"
    assert restored_variant.sample_values == {"bar", "foo"}

    assert len(restored.schema_mapping) == 1
    restored_entry = restored.schema_mapping[0]
    assert restored_entry.canonical_name == "col0_norm"
    assert restored_entry.source_index == 0
    assert restored_entry.target_index == 0

    assert len(restored.column_profiles) == 1
    restored_profile = restored.column_profiles[0]
    assert restored_profile.header == "col0"
    assert restored_profile.unique_estimate == 2


def test_legacy_mapping_payloads_upgrade_versions():
    mapping = _build_mapping()
    payload = mapping.to_dict(include_samples=True)
    payload.pop("artifact_version", None)
    for cluster in payload.get("header_clusters", []):
        cluster.pop("version", None)
    restored = MappingConfig.from_dict(payload)
    assert restored.artifact_version == MAPPING_ARTIFACT_VERSION
    assert restored.header_clusters[0].version == HEADER_CLUSTER_VERSION
    upgraded = restored.to_dict()
    assert upgraded["artifact_version"] == MAPPING_ARTIFACT_VERSION
    assert upgraded["header_clusters"][0]["version"] == HEADER_CLUSTER_VERSION
