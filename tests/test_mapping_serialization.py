from __future__ import annotations

from pathlib import Path

from common.models import (
    ColumnStats,
    FileBlock,
    MappingConfig,
    SchemaColumn,
    SchemaDefinition,
    SchemaSignature,
)


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
    return MappingConfig(blocks=[block], schemas=[schema])


def test_mapping_config_to_dict_excludes_samples_by_default():
    payload = _build_mapping().to_dict()
    column_payload = payload["blocks"][0]["signature"]["columns"]["0"]
    assert "sample_values" not in column_payload


def test_mapping_config_round_trip_with_samples():
    mapping = _build_mapping()
    payload = mapping.to_dict(include_samples=True)
    column_payload = payload["blocks"][0]["signature"]["columns"]["0"]
    assert sorted(column_payload["sample_values"]) == ["bar", "foo"]

    restored = MappingConfig.from_dict(payload)
    restored_stats = restored.blocks[0].signature.columns[0]
    assert restored_stats.sample_values == {"bar", "foo"}
    assert restored.schemas[0].columns[0].raw_name == "col0"
