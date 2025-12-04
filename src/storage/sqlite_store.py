"""SQLite persistence for schema metadata and audit trails."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from common.models import (
    ColumnProfileResult,
    FileHeaderSummary,
    FileProgress,
    HeaderOccurrence,
    HeaderTypeProfile,
    JobMetrics,
    JobStatusRecord,
    JobProgressEvent,
    MappingConfig,
)


SCHEMA_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at REAL NOT NULL
)
"""

MIGRATIONS: List[tuple[int, List[str]]] = [
    (
        1,
        [
            """
            CREATE TABLE IF NOT EXISTS schemas (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                columns_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_schemas_updated_at ON schemas(updated_at)
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
            CREATE INDEX IF NOT EXISTS idx_blocks_schema_block ON blocks(schema_id, block_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_blocks_file_path ON blocks(file_path)
            """,
            """
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schema_id TEXT NOT NULL,
                column_name TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(schema_id) REFERENCES schemas(id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_stats_schema_column ON stats(schema_id, column_name)
            """,
            """
            CREATE TABLE IF NOT EXISTS synonyms (
                canonical_name TEXT NOT NULL,
                variant TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (canonical_name, variant)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_synonyms_variant ON synonyms(variant)
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
            CREATE INDEX IF NOT EXISTS idx_job_metrics_schema ON job_metrics(schema_id)
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
            """
            CREATE INDEX IF NOT EXISTS idx_job_progress_schema ON job_progress_events(schema_id, created_at)
            """,
        ],
    ),
    (
        2,
        [
            """
            CREATE TABLE IF NOT EXISTS header_occurrences (
                raw_header TEXT NOT NULL,
                file_id TEXT NOT NULL,
                column_index INTEGER NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_header_occurrences_file ON header_occurrences(file_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS header_profiles (
                raw_header TEXT PRIMARY KEY,
                type_profile_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS file_headers (
                file_id TEXT PRIMARY KEY,
                headers_json TEXT NOT NULL
            )
            """,
        ],
    ),
    (
        3,
        [
            """
            CREATE TABLE IF NOT EXISTS column_profiles (
                file_id TEXT NOT NULL,
                column_index INTEGER NOT NULL,
                header TEXT,
                type_distribution_json TEXT NOT NULL,
                unique_estimate INTEGER NOT NULL,
                null_count INTEGER NOT NULL,
                total_values INTEGER NOT NULL,
                numeric_min REAL,
                numeric_max REAL,
                date_min TEXT,
                date_max TEXT,
                PRIMARY KEY (file_id, column_index)
            )
            """,
        ],
    ),
    (
        4,
        [
            """
            CREATE TABLE IF NOT EXISTS job_status (
                job_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                detail TEXT,
                last_error TEXT,
                metadata_json TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS job_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                state TEXT NOT NULL,
                detail TEXT,
                created_at REAL NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, created_at)
            """,
        ],
    ),
]

MAX_PROGRESS_EVENTS_PER_SCHEMA = 500


def initialize(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        _apply_migrations(conn)


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


def persist_header_metadata(
    db_path: Path,
    file_headers: Sequence[FileHeaderSummary],
    occurrences: Sequence[HeaderOccurrence],
    profiles: Sequence[HeaderTypeProfile],
) -> None:
    initialize(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM file_headers")
        conn.execute("DELETE FROM header_occurrences")
        conn.execute("DELETE FROM header_profiles")
        if file_headers:
            conn.executemany(
                "INSERT INTO file_headers(file_id, headers_json) VALUES (?, ?)",
                ((item.file_id, json.dumps(item.headers, ensure_ascii=False)) for item in file_headers),
            )
        if occurrences:
            conn.executemany(
                "INSERT INTO header_occurrences(raw_header, file_id, column_index) VALUES (?, ?, ?)",
                ((item.raw_header, item.file_id, item.column_index) for item in occurrences),
            )
        if profiles:
            conn.executemany(
                "INSERT INTO header_profiles(raw_header, type_profile_json) VALUES (?, ?)",
                (
                    (item.raw_header, json.dumps(item.type_profile, ensure_ascii=False))
                    for item in profiles
                ),
            )
        conn.commit()


def persist_column_profiles(db_path: Path, profiles: Sequence[ColumnProfileResult]) -> None:
    initialize(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM column_profiles")
        if profiles:
            conn.executemany(
                """
                INSERT INTO column_profiles(
                    file_id,
                    column_index,
                    header,
                    type_distribution_json,
                    unique_estimate,
                    null_count,
                    total_values,
                    numeric_min,
                    numeric_max,
                    date_min,
                    date_max
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        item.file_id,
                        item.column_index,
                        item.header,
                        json.dumps(item.type_distribution, ensure_ascii=False),
                        item.unique_estimate,
                        item.null_count,
                        item.total_values,
                        item.numeric_min,
                        item.numeric_max,
                        item.date_min,
                        item.date_max,
                    )
                    for item in profiles
                ),
            )
        conn.commit()


def fetch_column_profiles(db_path: Path) -> List[ColumnProfileResult]:
    initialize(db_path)
    items: List[ColumnProfileResult] = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            SELECT file_id, column_index, header, type_distribution_json, unique_estimate,
                   null_count, total_values, numeric_min, numeric_max, date_min, date_max
            FROM column_profiles
            ORDER BY file_id, column_index
            """
        )
        for (
            file_id,
            column_index,
            header,
            type_json,
            unique_estimate,
            null_count,
            total_values,
            numeric_min,
            numeric_max,
            date_min,
            date_max,
        ) in cursor.fetchall():
            items.append(
                ColumnProfileResult(
                    file_id=str(file_id or ""),
                    column_index=int(column_index),
                    header=str(header or ""),
                    type_distribution={
                        str(k): int(v)
                        for k, v in json.loads(type_json or "{}").items()
                    },
                    unique_estimate=int(unique_estimate or 0),
                    null_count=int(null_count or 0),
                    total_values=int(total_values or 0),
                    numeric_min=float(numeric_min) if numeric_min is not None else None,
                    numeric_max=float(numeric_max) if numeric_max is not None else None,
                    date_min=str(date_min) if date_min else None,
                    date_max=str(date_max) if date_max else None,
                )
            )
    return items


def fetch_file_headers(db_path: Path) -> List[FileHeaderSummary]:
    initialize(db_path)
    summaries: List[FileHeaderSummary] = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT file_id, headers_json FROM file_headers")
        for file_id, headers_json in cursor.fetchall():
            headers = json.loads(headers_json) if headers_json else []
            summaries.append(FileHeaderSummary(file_id=file_id, headers=[str(h) for h in headers]))
    return summaries


def fetch_header_occurrences(db_path: Path) -> List[HeaderOccurrence]:
    initialize(db_path)
    items: List[HeaderOccurrence] = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT raw_header, file_id, column_index FROM header_occurrences ORDER BY file_id, column_index"
        )
        for raw_header, file_id, column_index in cursor.fetchall():
            items.append(
                HeaderOccurrence(
                    raw_header=str(raw_header or ""),
                    file_id=str(file_id or ""),
                    column_index=int(column_index),
                )
            )
    return items


def fetch_header_profiles(db_path: Path) -> List[HeaderTypeProfile]:
    initialize(db_path)
    profiles: List[HeaderTypeProfile] = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT raw_header, type_profile_json FROM header_profiles")
        for raw_header, payload in cursor.fetchall():
            data = json.loads(payload) if payload else {}
            profiles.append(
                HeaderTypeProfile(
                    raw_header=str(raw_header or ""),
                    type_profile={str(k): int(v) for k, v in data.items()},
                )
            )
    return profiles


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
            "missing_required": validation.missing_required,
            "type_mismatches": validation.type_mismatches,
        },
        "spill": {
            "spills": spill.spills,
            "rows_spilled": spill.rows_spilled,
            "bytes_spilled": spill.bytes_spilled,
            "max_buffer_rows": spill.max_buffer_rows,
        },
    }
    error_count = (
        validation.short_rows
        + validation.long_rows
        + validation.missing_required
        + validation.type_mismatches
    )
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


def upsert_job_status(
    db_path: Path,
    job_id: str,
    state: str,
    *,
    detail: str | None = None,
    last_error: str | None = None,
    metadata: Optional[Dict[str, object]] = None,
) -> JobStatusRecord:
    initialize(db_path)
    payload = json.dumps(metadata, ensure_ascii=False) if metadata else None
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT created_at FROM job_status WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        created_at = row[0] if row else now
        if row:
            conn.execute(
                """
                UPDATE job_status
                SET state = ?, detail = ?, last_error = ?, metadata_json = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (state, detail, last_error, payload, now, job_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO job_status(job_id, state, detail, last_error, metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, state, detail, last_error, payload, created_at, now),
            )
        conn.commit()
    return JobStatusRecord(
        job_id=job_id,
        state=state,
        detail=detail,
        last_error=last_error,
        metadata=metadata or {},
        created_at=float(created_at),
        updated_at=float(now),
    )


def record_job_event(db_path: Path, job_id: str, state: str, detail: str | None = None) -> None:
    initialize(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO job_events(job_id, state, detail, created_at) VALUES (?, ?, ?, ?)",
            (job_id, state, detail, time.time()),
        )
        conn.commit()


def fetch_job_status(db_path: Path, job_id: str) -> Optional[JobStatusRecord]:
    initialize(db_path)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            SELECT job_id, state, detail, last_error, metadata_json, created_at, updated_at
            FROM job_status
            WHERE job_id = ?
            """,
            (job_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
    metadata_payload = json.loads(row[4]) if row[4] else {}
    return JobStatusRecord(
        job_id=row[0],
        state=row[1],
        detail=row[2],
        last_error=row[3],
        metadata={str(k): v for k, v in metadata_payload.items()},
        created_at=float(row[5] or 0.0),
        updated_at=float(row[6] or 0.0),
    )


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


def _apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(SCHEMA_MIGRATIONS_TABLE)
    applied_versions = {
        row[0]
        for row in conn.execute("SELECT version FROM schema_migrations")
    }
    for version, statements in sorted(MIGRATIONS, key=lambda item: item[0]):
        if version in applied_versions:
            continue
        for statement in statements:
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (version, time.time()),
        )
        conn.commit()
