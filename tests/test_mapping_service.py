"""Tests for schema clustering and normalization hooks."""
from __future__ import annotations

from pathlib import Path

from common.models import ColumnStats, FileBlock, MappingConfig, SchemaSignature
from core.mapping import MappingService
from core.normalization import SynonymDictionary


def test_cluster_assigns_schema_ids_and_normalized_names() -> None:
    signature = SchemaSignature(
        delimiter=",",
        column_count=2,
        header_sample="Customer Id,Order Total",
        columns={
            0: ColumnStats(index=0, maybe_numeric=True, maybe_bool=False, maybe_date=False),
            1: ColumnStats(index=1, maybe_numeric=True, maybe_bool=False, maybe_date=False),
        },
    )

    block_a = FileBlock(
        file_path=Path("tests/data/retail_small.csv"),
        block_id=0,
        start_line=0,
        end_line=999,
        signature=signature,
    )

    block_b = FileBlock(
        file_path=Path("tests/data/retail_small.csv"),
        block_id=1,
        start_line=1000,
        end_line=1999,
        signature=signature,
    )

    synonyms = SynonymDictionary.from_mapping({"customer_id": ["customer id"], "order_total": ["order total"]})
    service = MappingService(synonyms)
    result = service.cluster([block_a, block_b])

    assert len(result.schemas) == 1
    schema = result.schemas[0]
    assert all(block.schema_id == schema.id for block in result.blocks)
    normalized_names = [col.normalized_name for col in schema.columns]
    assert normalized_names == ["customer_id", "order_total"]
