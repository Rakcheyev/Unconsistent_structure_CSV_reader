"""SQLite persistence for schema metadata and audit trails."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Iterable, List

from common.models import FileProgress, JobMetrics, JobProgressEvent, MappingConfig


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS schemas (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        columns_json TEXT NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS blocks (
        block_key TEXT PRIMARY KEY,
        file_path TEXT NOT NULL,
        block_id INTEGER NOT NULL,
        schema_id TEXT,
        start_line INTEGER NOT NULL,
        end_line INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity TEXT NOT NULL,
        action TEXT NOT NULL,
        detail TEXT,
        created_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        schema_id TEXT NOT NULL,
        schema_name TEXT,
        rows_written INTEGER NOT NULL,
        duration_seconds REAL NOT NULL,
        rows_per_second REAL NOT NULL,
        error_count INTEGER NOT NULL,
        warnings_json TEXT,
        spill_count INTEGER NOT NULL,
        rows_spilled INTEGER NOT NULL,
        created_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job_progress_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        schema_id TEXT NOT NULL,
        schema_name TEXT,
        file_path TEXT NOT NULL,
        processed_rows INTEGER NOT NULL,
        total_rows INTEGER,
        eta_seconds REAL,
        rows_per_second REAL,
        spill_rows INTEGER,
        created_at REAL NOT NULL
    )
    """,
]

MAX_PROGRESS_EVENTS_PER_SCHEMA = 500


def initialize(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        conn.commit()


def persist_mapping(mapping: MappingConfig, db_path: Path) -> None:
    initialize(db_path)
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM schemas")
        conn.execute("DELETE FROM blocks")
        for schema in mapping.schemas:
            conn.execute(
                "INSERT OR REPLACE INTO schemas(id, name, columns_json, updated_at) VALUES (?, ?, ?, ?)",
                (
                    str(schema.id),
                    schema.name,
                    json.dumps([col.__dict__ for col in schema.columns], ensure_ascii=False),
                    now,
                ),
            )
        for block in mapping.blocks:
            block_key = f"{block.file_path}:{block.block_id}"
            conn.execute(
                """
                INSERT OR REPLACE INTO blocks(block_key, file_path, block_id, schema_id, start_line, end_line)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    block_key,
                    str(block.file_path),
                    block.block_id,
                    str(block.schema_id) if block.schema_id else None,
                    block.start_line,
                    block.end_line,
                ),
            )
        conn.commit()


def record_audit_event(db_path: Path, entity: str, action: str, detail: str | None = None) -> None:
    initialize(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO audit_log(entity, action, detail, created_at) VALUES (?, ?, ?, ?)",
            (entity, action, detail, time.time()),
        )
        conn.commit()


def record_job_metrics(db_path: Path, metrics: JobMetrics) -> None:
    initialize(db_path)
    validation = metrics.validation
    spill = metrics.spill_metrics
    warning_payload = {
        "validation": {
            "total_rows": validation.total_rows,
            "short_rows": validation.short_rows,
            "long_rows": validation.long_rows,
            "empty_rows": validation.empty_rows,
        },
        "spill": {
            "spills": spill.spills,
            "rows_spilled": spill.rows_spilled,
            "bytes_spilled": spill.bytes_spilled,
            "max_buffer_rows": spill.max_buffer_rows,
        },
    }
    error_count = validation.short_rows + validation.long_rows
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO job_metrics(
                schema_id,
                schema_name,
                rows_written,
                duration_seconds,
                rows_per_second,
                error_count,
                warnings_json,
                spill_count,
                rows_spilled,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metrics.schema_id,
                metrics.schema_name,
                metrics.rows_written,
                metrics.duration_seconds,
                metrics.rows_per_second,
                error_count,
                json.dumps(warning_payload, ensure_ascii=False),
                spill.spills,
                spill.rows_spilled,
                time.time(),
            ),
        )
        conn.commit()


def record_progress_event(db_path: Path, progress: FileProgress) -> None:
    initialize(db_path)
    schema_id = progress.schema_id or progress.file_path.stem
    schema_name = progress.schema_name or None
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO job_progress_events(
                schema_id,
                schema_name,
                file_path,
                processed_rows,
                total_rows,
                eta_seconds,
                rows_per_second,
                spill_rows,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                schema_id,
                schema_name,
                progress.file_path.as_posix(),
                progress.processed_rows,
                progress.total_rows,
                progress.eta_seconds,
                progress.rows_per_second,
                progress.spill_rows,
                time.time(),
            ),
        )
        _prune_progress_events(conn, schema_id)
        conn.commit()


def fetch_job_progress_events(
    db_path: Path,
    *,
    schema_id: str | None = None,
    limit: int = 100,
) -> List[JobProgressEvent]:
    initialize(db_path)
    clause = "WHERE schema_id = ?" if schema_id else ""
    params: List[object] = [schema_id] if schema_id else []
    params.append(limit)
    query = f"""
        SELECT schema_id, schema_name, file_path, processed_rows, total_rows,
               eta_seconds, rows_per_second, spill_rows, created_at
        FROM job_progress_events
        {clause}
        ORDER BY created_at DESC
        LIMIT ?
    """
    events: List[JobProgressEvent] = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(query, params)
        for row in cursor.fetchall():
            events.append(
                JobProgressEvent(
                    schema_id=row[0],
                    schema_name=row[1],
                    file_path=Path(row[2]),
                    processed_rows=row[3],
                    total_rows=row[4],
                    eta_seconds=row[5],
                    rows_per_second=row[6],
                    spill_rows=row[7],
                    created_at=row[8],
                )
            )
    return events


def prune_progress_history(
    db_path: Path,
    *,
    schema_id: str | None = None,
    max_per_schema: int = MAX_PROGRESS_EVENTS_PER_SCHEMA,
) -> None:
    initialize(db_path)
    with sqlite3.connect(db_path) as conn:
        if schema_id:
            _prune_progress_events(conn, schema_id, max_per_schema)
        else:
            cursor = conn.execute("SELECT DISTINCT schema_id FROM job_progress_events")
            schema_ids = [row[0] for row in cursor.fetchall()]
            for sid in schema_ids:
                _prune_progress_events(conn, sid, max_per_schema)
        conn.commit()


def _prune_progress_events(
    conn: sqlite3.Connection,
    schema_id: str,
    limit: int | None = None,
) -> None:
    effective_limit = MAX_PROGRESS_EVENTS_PER_SCHEMA if limit is None else limit
    if effective_limit <= 0:
        conn.execute("DELETE FROM job_progress_events WHERE schema_id = ?", (schema_id,))
        return
    conn.execute(
        """
        DELETE FROM job_progress_events
        WHERE schema_id = ?
          AND id NOT IN (
                SELECT id FROM job_progress_events
                WHERE schema_id = ?
                ORDER BY created_at DESC
                LIMIT ?
          )
        """,
        (schema_id, schema_id, effective_limit),
    )
