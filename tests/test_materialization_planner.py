"""Tests for materialization plan generation."""
from __future__ import annotations

from pathlib import Path

from common.models import FileBlock, MappingConfig, SchemaDefinition, SchemaSignature
from core.materialization import MaterializationPlanner


def test_plan_groups_blocks_by_schema() -> None:
    schema = SchemaDefinition(name="orders")
    signature = SchemaSignature(delimiter=",", column_count=1, header_sample="id")

    block = FileBlock(
        file_path=Path("tests/data/retail_small.csv"),
        block_id=0,
        start_line=0,
        end_line=9,
        signature=signature,
        schema_id=schema.id,
    )

    mapping = MappingConfig(blocks=[block], schemas=[schema])
    planner = MaterializationPlanner(chunk_rows=100000)
    plan = planner.build_plan(mapping, Path("artifacts"))

    assert len(plan) == 1
    entry = plan[0]
    assert entry.schema_id == str(schema.id)
    assert entry.block_count == 1
    assert entry.estimated_rows == 10
