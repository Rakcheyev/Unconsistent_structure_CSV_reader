from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow is required for parquet writer tests")

from common.models import (
    CanonicalColumnSpec,
    CanonicalSchema,
    CanonicalSchemaRegistry,
    FileBlock,
    FileProgress,
    MappingConfig,
    ProfileSettings,
    ResourceLimits,
    RuntimeConfig,
    SchemaColumn,
    SchemaDefinition,
    SchemaSignature,
    GlobalSettings,
)
from core.materialization.runner import MaterializationJobRunner
from core.resources import ResourceManager


def build_runtime(chunk_rows: int = 2) -> RuntimeConfig:
    return RuntimeConfig(
        global_settings=GlobalSettings(
            encoding="utf-8",
            error_policy="replace",
            synonym_dictionary="storage/synonyms.json",
        ),
        profile=ProfileSettings(
            description="test",
            block_size=1024,
            min_gap_lines=1,
            max_parallel_files=2,
            sample_values_cap=10,
            writer_chunk_rows=chunk_rows,
        ),
    )


def test_materialization_runner_writes_chunked_files(tmp_path: Path) -> None:
    input_csv = tmp_path / "customers.csv"
    input_csv.write_text(
        "name,email\nAlice,a@example.com\nBob\nCharlie,c@example.com\n",
        encoding="utf-8",
    )
    schema_id = uuid4()
    schema = SchemaDefinition(
        id=schema_id,
        name="customers",
        columns=[
            SchemaColumn(index=0, raw_name="name", normalized_name="name"),
            SchemaColumn(index=1, raw_name="email", normalized_name="email"),
        ],
    )
    block = FileBlock(
        file_path=input_csv,
        block_id=0,
        start_line=0,
        end_line=3,
        signature=SchemaSignature(delimiter=",", column_count=2, header_sample="name,email"),
        schema_id=schema_id,
    )
    mapping = MappingConfig(blocks=[block], schemas=[schema])

    runner = MaterializationJobRunner(
        build_runtime(chunk_rows=2),
        writer_format="csv",
        spill_threshold=1,
    )
    summaries = runner.run(mapping, dest_dir=tmp_path / "out", max_jobs=1)

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.rows_written == 3
    assert summary.validation.short_rows == 1
    assert summary.validation.long_rows == 0
    assert summary.spill_metrics.spills >= 1
    output_dir = tmp_path / "out"
    files = sorted(output_dir.glob("*.csv"))
    assert len(files) == 2  # chunk of 2 rows -> 2 files for 3 rows
    first_file = files[0].read_text(encoding="utf-8").strip().splitlines()
    assert first_file[0] == "name,email"
    assert "Alice,a@example.com" in first_file
    second_file = files[1].read_text(encoding="utf-8").strip().splitlines()
    assert second_file[0] == "name,email"
    assert "Charlie,c@example.com" in second_file


def test_materialization_runner_parquet_writer(tmp_path: Path) -> None:
    input_csv = tmp_path / "orders.csv"
    input_csv.write_text(
        "id,total\n1,10.5\n2,20.0\n3,30.1\n",
        encoding="utf-8",
    )
    schema_id = uuid4()
    schema = SchemaDefinition(
        id=schema_id,
        name="orders",
        columns=[
            SchemaColumn(index=0, raw_name="id", normalized_name="id"),
            SchemaColumn(index=1, raw_name="total", normalized_name="total"),
        ],
    )
    block = FileBlock(
        file_path=input_csv,
        block_id=0,
        start_line=0,
        end_line=3,
        signature=SchemaSignature(delimiter=",", column_count=2, header_sample="id,total"),
        schema_id=schema_id,
    )
    runner = MaterializationJobRunner(
        build_runtime(chunk_rows=2),
        writer_format="parquet",
    )
    summaries = runner.run(MappingConfig(blocks=[block], schemas=[schema]), dest_dir=tmp_path / "out", max_jobs=1)
    assert summaries[0].rows_written == 3
    parquet_files = list((tmp_path / "out").glob("*.parquet"))
    assert parquet_files
    total_rows = sum(pq.read_table(path).num_rows for path in parquet_files)
    assert total_rows == 3


def test_materialization_runner_database_writer(tmp_path: Path) -> None:
    input_csv = tmp_path / "users.csv"
    input_csv.write_text("name,email\nJane,jane@example.com\nJohn,john@example.com\n", encoding="utf-8")
    schema_id = uuid4()
    schema = SchemaDefinition(
        id=schema_id,
        name="users",
        columns=[
            SchemaColumn(index=0, raw_name="name", normalized_name="name"),
            SchemaColumn(index=1, raw_name="email", normalized_name="email"),
        ],
    )
    block = FileBlock(
        file_path=input_csv,
        block_id=0,
        start_line=0,
        end_line=2,
        signature=SchemaSignature(delimiter=",", column_count=2, header_sample="name,email"),
        schema_id=schema_id,
    )
    db_path = tmp_path / "warehouse.db"
    runner = MaterializationJobRunner(
        build_runtime(chunk_rows=2),
        writer_format="database",
        db_url=f"sqlite:///{db_path}",
    )
    runner.run(MappingConfig(blocks=[block], schemas=[schema]), dest_dir=tmp_path / "out", max_jobs=1)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute('SELECT name, email FROM "users" ORDER BY name').fetchall()
    assert rows == [("Jane", "jane@example.com"), ("John", "john@example.com")]


def test_materialization_runner_emits_progress(tmp_path: Path) -> None:
    input_csv = tmp_path / "events.csv"
    input_csv.write_text("value\n1\n2\n3\n4\n", encoding="utf-8")
    schema_id = uuid4()
    schema = SchemaDefinition(
        id=schema_id,
        name="events",
        columns=[SchemaColumn(index=0, raw_name="value", normalized_name="value")],
    )
    block = FileBlock(
        file_path=input_csv,
        block_id=0,
        start_line=0,
        end_line=4,
        signature=SchemaSignature(delimiter=",", column_count=1, header_sample="value"),
        schema_id=schema_id,
    )
    events: list[FileProgress] = []

    def progress_cb(progress: FileProgress) -> None:
        events.append(progress)

    runner = MaterializationJobRunner(
        build_runtime(chunk_rows=2),
        writer_format="csv",
    )
    runner.run(
        MappingConfig(blocks=[block], schemas=[schema]),
        dest_dir=tmp_path / "out",
        max_jobs=1,
        progress_callback=progress_cb,
    )
    assert events
    final = events[-1]
    assert final.total_rows >= final.processed_rows
    assert final.schema_id == str(schema_id)
    assert final.rows_per_second is not None


def test_materialization_runner_enforces_canonical_contract(tmp_path: Path) -> None:
    input_csv = tmp_path / "customers_contract.csv"
    input_csv.write_text(
        "name,email,age\n"
        "Alice,a@example.com,30\n"
        "Bob,,thirty\n",
        encoding="utf-8",
    )
    schema_id = uuid4()
    schema = SchemaDefinition(
        id=schema_id,
        name="retail_orders",
        canonical_schema_id="retail_orders",
        canonical_namespace="retail",
        columns=[
            SchemaColumn(index=0, raw_name="name", normalized_name="name"),
            SchemaColumn(index=1, raw_name="email", normalized_name="email"),
            SchemaColumn(index=2, raw_name="age", normalized_name="age"),
        ],
    )
    block = FileBlock(
        file_path=input_csv,
        block_id=0,
        start_line=0,
        end_line=2,
        signature=SchemaSignature(delimiter=",", column_count=3, header_sample="name,email,age"),
        schema_id=schema_id,
    )
    registry = CanonicalSchemaRegistry()
    registry.register(
        CanonicalSchema(
            schema_id="retail_orders",
            display_name="Retail Orders",
            version="1",
            namespace="retail",
            columns=[
                CanonicalColumnSpec(name="name", data_type="string", required=True, allow_null=False),
                CanonicalColumnSpec(name="email", data_type="string", required=True, allow_null=False),
                CanonicalColumnSpec(name="age", data_type="int", required=False, allow_null=True, min_value=0.0),
            ],
        )
    )

    runner = MaterializationJobRunner(
        build_runtime(chunk_rows=2),
        writer_format="csv",
        canonical_registry=registry,
    )
    summary = runner.run(
        MappingConfig(blocks=[block], schemas=[schema]),
        dest_dir=tmp_path / "out_contract",
        max_jobs=1,
    )[0]

    assert summary.validation.missing_required == 1
    assert summary.validation.type_mismatches == 1


def test_materialization_runner_uses_resource_manager_scratch(tmp_path: Path) -> None:
    input_csv = tmp_path / "orders.csv"
    input_csv.write_text("id,total\n1,10\n", encoding="utf-8")
    schema_id = uuid4()
    schema = SchemaDefinition(
        id=schema_id,
        name="orders",
        columns=[
            SchemaColumn(index=0, raw_name="id", normalized_name="id"),
            SchemaColumn(index=1, raw_name="total", normalized_name="total"),
        ],
    )
    block = FileBlock(
        file_path=input_csv,
        block_id=0,
        start_line=0,
        end_line=1,
        signature=SchemaSignature(delimiter=",", column_count=2, header_sample="id,total"),
        schema_id=schema_id,
    )
    scratch_root = tmp_path / "scratch"
    resource_manager = ResourceManager(ResourceLimits(temp_dir=str(scratch_root), max_workers=1))
    runner = MaterializationJobRunner(
        build_runtime(chunk_rows=1),
        writer_format="csv",
        job_id="job-test",
        resource_manager=resource_manager,
    )
    runner.run(MappingConfig(blocks=[block], schemas=[schema]), dest_dir=tmp_path / "out", max_jobs=1)
    expected_dir = scratch_root / "job-test" / "materialize" / "orders"
    assert expected_dir.exists()
