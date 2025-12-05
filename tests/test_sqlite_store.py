from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from common.models import (
    ColumnProfileResult,
    FileProgress,
    JobMetrics,
    MappingConfig,
    SchemaColumn,
    SchemaDefinition,
    SpillMetrics,
    ValidationSummary,
)
from common.versioning import HEADER_CLUSTER_VERSION, MAPPING_ARTIFACT_VERSION
from storage import (
    fetch_column_profiles,
    fetch_job_progress_events,
    persist_column_profiles,
    prune_progress_history,
)
from storage.sqlite_store import (
    MIGRATIONS,
    initialize,
    persist_mapping,
    record_job_metrics,
    record_progress_event,
)


def test_record_job_metrics_persists_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "storage.db"
    initialize(db_path)
    metrics = JobMetrics(
        schema_id="schema-1",
        schema_name="customers",
        rows_written=120,
        duration_seconds=4.0,
        rows_per_second=30.0,
        validation=ValidationSummary(total_rows=120, short_rows=2, long_rows=1, empty_rows=5),
        spill_metrics=SpillMetrics(spills=3, rows_spilled=45, bytes_spilled=1024, max_buffer_rows=100),
    )
    record_job_metrics(db_path, metrics)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT schema_id, rows_written, rows_per_second, error_count, warnings_json, spill_count FROM job_metrics"
        )
        row = cursor.fetchone()
    assert row is not None
    schema_id, rows_written, rows_per_second, error_count, warnings_json, spill_count = row
    assert schema_id == "schema-1"
    assert rows_written == 120
    assert rows_per_second == 30.0
    assert error_count == 3  # short + long rows
    assert spill_count == 3
    payload = json.loads(warnings_json)
    assert payload["validation"]["short_rows"] == 2
    assert payload["spill"]["rows_spilled"] == 45


def test_record_progress_event_appends_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "storage.db"
    initialize(db_path)
    progress = FileProgress(
        file_path=tmp_path / "artifacts" / "customers.materialize",
        processed_rows=500,
        total_rows=1_000,
        current_phase="materialize",
        eta_seconds=12.5,
        schema_id="schema-1",
        schema_name="customers",
        rows_per_second=250.0,
        spill_rows=20,
    )
    record_progress_event(db_path, progress)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT schema_id, processed_rows, total_rows, eta_seconds, rows_per_second, spill_rows FROM job_progress_events"
        ).fetchone()
    assert row is not None
    schema_id, processed_rows, total_rows, eta_seconds, rows_per_second, spill_rows = row
    assert schema_id == "schema-1"
    assert processed_rows == 500
    assert total_rows == 1_000
    assert eta_seconds == 12.5
    assert rows_per_second == 250.0
    assert spill_rows == 20


def test_fetch_job_progress_events_returns_latest(tmp_path: Path) -> None:
    db_path = tmp_path / "storage.db"
    initialize(db_path)
    for idx in range(3):
        progress = FileProgress(
            file_path=tmp_path / f"job_{idx}.materialize",
            processed_rows=idx * 10,
            total_rows=100,
            current_phase="materialize",
            eta_seconds=float(100 - idx * 10),
            schema_id="schema-1",
            schema_name="customers",
            rows_per_second=100.0 + idx,
            spill_rows=idx,
        )
        record_progress_event(db_path, progress)

    events = fetch_job_progress_events(db_path, schema_id="schema-1", limit=2)
    assert len(events) == 2
    assert events[0].processed_rows > events[1].processed_rows
    assert events[0].file_path.name == "job_2.materialize"


def test_progress_event_retention_enforced(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "storage.db"
    initialize(db_path)
    from storage import sqlite_store

    monkeypatch.setattr(sqlite_store, "MAX_PROGRESS_EVENTS_PER_SCHEMA", 3)
    for idx in range(5):
        record_progress_event(
            db_path,
            FileProgress(
                file_path=tmp_path / f"job_{idx}.materialize",
                processed_rows=idx,
                total_rows=100,
                current_phase="materialize",
                schema_id="schema-1",
                schema_name="customers",
            ),
        )

    events = fetch_job_progress_events(db_path, schema_id="schema-1", limit=10)
    assert len(events) == 3
    assert events[0].processed_rows == 4
    assert events[-1].processed_rows == 2

    # manual prune API can trim other schemas as needed
    record_progress_event(
        db_path,
        FileProgress(
            file_path=tmp_path / "job_other.materialize",
            processed_rows=1,
            total_rows=5,
            current_phase="materialize",
            schema_id="schema-2",
        ),
    )
    prune_progress_history(db_path, schema_id="schema-2", max_per_schema=0)
    other = fetch_job_progress_events(db_path, schema_id="schema-2")
    assert other == []


def test_column_profiles_persist_and_fetch(tmp_path: Path) -> None:
    db_path = tmp_path / "profiles.db"
    profile = ColumnProfileResult(
        file_id="input.csv",
        column_index=1,
        header="amount",
        type_distribution={"integer": 2, "null": 1},
        unique_estimate=2,
        null_count=1,
        total_values=3,
        numeric_min=10.0,
        numeric_max=25.0,
        date_min=None,
        date_max=None,
    )
    persist_column_profiles(db_path, [profile])

    rebuilt = fetch_column_profiles(db_path)
    assert len(rebuilt) == 1
    restored = rebuilt[0]
    assert restored.header == "amount"
    assert restored.numeric_max == 25.0
    assert restored.type_distribution["integer"] == 2


def test_initialize_applies_all_migrations(tmp_path: Path) -> None:
    db_path = tmp_path / "migrations.db"
    initialize(db_path)

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        expected_tables = {
            "schemas",
            "blocks",
            "stats",
            "synonyms",
            "audit_log",
            "job_metrics",
            "job_progress_events",
            "schema_migrations",
            "artifact_metadata",
        }
        assert expected_tables.issubset(tables)

        schema_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('schemas')")
        }
        assert {
            "canonical_schema_id",
            "canonical_namespace",
            "canonical_schema_version",
        }.issubset(schema_columns)

        latest_version = MIGRATIONS[-1][0]
        applied_versions = {
            row[0]
            for row in conn.execute("SELECT version FROM schema_migrations")
        }
        assert latest_version in applied_versions


def test_persist_mapping_writes_version_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "mapping.db"
    schema = SchemaDefinition(name="orders", columns=[SchemaColumn(index=0, raw_name="id")])
    schema.canonical_schema_id = "orders_v1"
    schema.canonical_namespace = "retail"
    schema.canonical_schema_version = "1.2.3"
    mapping = MappingConfig(schemas=[schema])
    persist_mapping(mapping, db_path)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT canonical_schema_id, canonical_namespace, canonical_schema_version FROM schemas"
        ).fetchone()
        metadata = {
            key: value
            for key, value in conn.execute("SELECT key, value FROM artifact_metadata")
        }

    assert row == ("orders_v1", "retail", "1.2.3")
    assert metadata["mapping.artifact_version"] == MAPPING_ARTIFACT_VERSION
    assert metadata["header_clusters.version"] == HEADER_CLUSTER_VERSION