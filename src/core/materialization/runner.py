"""Materialization job runner with validation, telemetry, and resumable writers."""
from __future__ import annotations

from abc import ABC, abstractmethod
import csv
from datetime import date, datetime
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, TextIO, Tuple
from uuid import uuid4

from common.config import error_mode_from_policy
from common.models import (
    CanonicalSchema,
    CanonicalSchemaRegistry,
    ColumnProfileResult,
    FileBlock,
    FileProgress,
    JobMetrics,
    MappingConfig,
    RuntimeConfig,
    SchemaDefinition,
    SchemaMappingEntry,
    SpillMetrics,
    ValidationSummary,
)
from common.text import slugify
from core.validation.canonical import resolve_canonical_schema

try:  # pragma: no cover - optional dependency validated via tests
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]


@dataclass(slots=True)
class JobSummary:
    schema_id: str
    schema_name: str
    blocks_processed: int
    rows_written: int
    rows_per_second: float
    output_files: List[str]
    duration_seconds: float
    validation: ValidationSummary
    spill_metrics: SpillMetrics

    def to_job_metrics(self) -> JobMetrics:
        return JobMetrics(
            schema_id=self.schema_id,
            schema_name=self.schema_name,
            rows_written=self.rows_written,
            duration_seconds=self.duration_seconds,
            rows_per_second=self.rows_per_second,
            validation=self.validation,
            spill_metrics=self.spill_metrics,
        )


class RowNormalizer:
    """Aligns rows to the canonical schema order using schema_mapping entries."""

    def __init__(
        self,
        mappings: Sequence[SchemaMappingEntry] | None = None,
        *,
        column_profiles: Sequence[ColumnProfileResult] | None = None,
    ) -> None:
        self._map = self._build_index(mappings or [])
        self._schema_slug_cache: Dict[str, Dict[str, int]] = {}
        self._profile_index = self._build_profile_index(column_profiles or [])

    def normalize(self, row: List[str], schema: SchemaDefinition, *, source_path: Path) -> "NormalizedRow":
        observed_length = len(row)
        mapping = self._map.get(self._key(source_path))
        if not mapping:
            return NormalizedRow(list(row), observed_length)

        max_target = mapping["max_target"]
        working_width = max(observed_length, max_target + 1)
        working_width = max(working_width, 1)
        normalized = [""] * working_width
        assigned = [False] * working_width
        used_sources: set[int] = set()

        for source_index, entry in mapping["entries"].items():
            target_index = self._resolve_target_index(entry, schema)
            if target_index is None:
                continue
            if target_index >= working_width:
                extension = target_index - working_width + 1
                normalized.extend([""] * extension)
                assigned.extend([False] * extension)
                working_width = len(normalized)
            value = row[source_index] if source_index < len(row) else ""
            normalized[target_index] = value
            assigned[target_index] = True
            used_sources.add(source_index)

        remainder = [row[idx] for idx in range(len(row)) if idx not in used_sources]
        remainder_iter = iter(remainder)
        for idx in range(len(normalized)):
            if assigned[idx]:
                continue
            normalized[idx] = next(remainder_iter, "")
            assigned[idx] = True
        return NormalizedRow(normalized, observed_length)

    def _resolve_target_index(
        self, entry: SchemaMappingEntry, schema: SchemaDefinition
    ) -> int | None:
        if entry.target_index is not None:
            return entry.target_index
        schema_id = str(schema.id)
        slug_map = self._schema_slug_cache.get(schema_id)
        if slug_map is None:
            slug_map = {}
            for column in schema.columns:
                slug = slugify(column.normalized_name or column.raw_name)
                if slug and slug not in slug_map:
                    slug_map[slug] = column.index
            self._schema_slug_cache[schema_id] = slug_map
        target = slug_map.get(slugify(entry.canonical_name))
        if target is not None:
            return target
        return self._match_by_type(entry, schema)

    def _key(self, path: Path) -> str:
        try:
            return path.resolve().as_posix()
        except OSError:
            return path.as_posix()

    def _build_index(
        self, mappings: Sequence[SchemaMappingEntry]
    ) -> Dict[str, Dict[str, object]]:
        index: Dict[str, Dict[str, object]] = {}
        for entry in mappings:
            file_key = self._key(entry.file_path)
            bucket = index.setdefault(file_key, {"entries": {}, "max_target": -1})
            bucket_entries = bucket["entries"]
            bucket_entries[entry.source_index] = entry
            if entry.target_index is not None:
                bucket["max_target"] = max(bucket["max_target"], entry.target_index)
        return index

    def _build_profile_index(
        self, profiles: Sequence[ColumnProfileResult]
    ) -> Dict[tuple[str, int], ColumnProfileResult]:
        index: Dict[tuple[str, int], ColumnProfileResult] = {}
        for profile in profiles:
            keys = {profile.file_id}
            try:
                keys.add(Path(profile.file_id).resolve().as_posix())
            except (OSError, ValueError):
                pass
            for key in keys:
                index[(key, profile.column_index)] = profile
        return index

    def _match_by_type(
        self, entry: SchemaMappingEntry, schema: SchemaDefinition
    ) -> int | None:
        if not self._profile_index:
            return None
        profile = self._profile_index.get((self._key(entry.file_path), entry.source_index))
        if profile is None:
            return None
        bucket = self._profile_bucket(profile)
        if not bucket:
            return None
        candidates = [
            column.index
            for column in schema.columns
            if self._schema_bucket(column.data_type) == bucket
        ]
        if not candidates and bucket in {"integer", "float"}:
            alt = "float" if bucket == "integer" else "integer"
            candidates = [
                column.index
                for column in schema.columns
                if self._schema_bucket(column.data_type) == alt
            ]
        return candidates[0] if candidates else None

    @staticmethod
    def _profile_bucket(profile: ColumnProfileResult) -> str | None:
        distribution = profile.type_distribution
        filtered = {k: v for k, v in distribution.items() if k != "null" and v > 0}
        if not filtered:
            return None
        return max(filtered.items(), key=lambda item: item[1])[0]

    @staticmethod
    def _schema_bucket(data_type: str | None) -> str:
        normalized = (data_type or "").lower()
        if normalized in {"int", "integer"}:
            return "integer"
        if normalized in {"float", "double", "decimal", "number"}:
            return "float"
        if "date" in normalized or "time" in normalized:
            return "date"
        return "text"


@dataclass(slots=True)
class NormalizedRow:
    values: List[str]
    observed_length: int


class MaterializationJobRunner:
    """Processes schemas into normalized datasets with validation + telemetry."""

    SUPPORTED_FORMATS = {"csv", "parquet", "database"}

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        checkpoint_path: Optional[Path] = None,
        writer_format: str = "csv",
        spill_threshold: int = 50_000,
        telemetry_log: Optional[Path] = None,
        db_url: Optional[str] = None,
        max_jobs: Optional[int] = None,
        canonical_registry: CanonicalSchemaRegistry | None = None,
    ) -> None:
        self.config = config
        self.encoding = config.global_settings.encoding
        self.errors = error_mode_from_policy(config.global_settings.error_policy)
        self.chunk_rows = max(1, config.profile.writer_chunk_rows)
        # max_jobs can be overridden, otherwise use profile
        self.max_jobs = max_jobs if max_jobs is not None else min(2, config.profile.max_parallel_files)
        self.checkpoints = CheckpointStore(checkpoint_path)
        self.writer_format = writer_format.lower() if writer_format else "csv"
        if self.writer_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported writer format '{writer_format}'.")
        self.spill_threshold = max(1, spill_threshold)
        self.telemetry_log = telemetry_log
        self.db_url = db_url
        self.progress_granularity = max(1_000, self.chunk_rows)
        self.canonical_registry = canonical_registry

    def run(
        self,
        mapping: MappingConfig,
        dest_dir: Path,
        *,
        max_jobs: Optional[int] = None,
        progress_callback: Optional[Callable[[FileProgress], None]] = None,
    ) -> List[JobSummary]:
        dest_dir.mkdir(parents=True, exist_ok=True)
        max_jobs = max_jobs or self.max_jobs or 1
        summaries: List[JobSummary] = []
        # Global dedup across all schemas for strict 1:1 invariant per source line.
        # Key: (str(file_path), line_number)
        global_seen_lines: set[tuple[str, int]] = set()
        schema_blocks: Dict[str, List[FileBlock]] = {}
        for block in mapping.blocks:
            if not block.schema_id:
                continue
            schema_blocks.setdefault(str(block.schema_id), []).append(block)
        schema_map = {str(schema.id): schema for schema in mapping.schemas}

        schema_mapping_entries = list(mapping.schema_mapping)
        column_profiles = list(mapping.column_profiles)
        with ThreadPoolExecutor(max_workers=max_jobs) as executor:
            futures = []
            for schema_id, blocks in schema_blocks.items():
                schema = schema_map.get(schema_id)
                if not schema:
                    continue
                futures.append(
                    executor.submit(
                        self._process_schema,
                        schema,
                        blocks,
                        dest_dir,
                        progress_callback,
                        global_seen_lines,
                        schema_mapping_entries,
                        column_profiles,
                    )
                )
            for future in futures:
                summaries.append(future.result())
        return summaries

    def _process_schema(
        self,
        schema: SchemaDefinition,
        blocks: List[FileBlock],
        dest_dir: Path,
        progress_callback: Optional[Callable[[FileProgress], None]],
        global_seen_lines: Optional[set[tuple[str, int]]] = None,
        schema_mappings: Sequence[SchemaMappingEntry] | None = None,
        column_profiles: Sequence[ColumnProfileResult] | None = None,
    ) -> JobSummary:
        blocks.sort(key=lambda b: (str(b.file_path), b.start_line))
        schema_id = str(schema.id)
        checkpoint = self.checkpoints.get(schema_id)
        start_block = checkpoint.get("next_block", 0) if checkpoint else 0
        canonical_schema = resolve_canonical_schema(schema, self.canonical_registry)
        writer = build_schema_writer(
            format_name=self.writer_format,
            schema=schema,
            dest_dir=dest_dir,
            chunk_rows=self.chunk_rows,
            encoding=self.encoding,
            errors=self.errors,
            checkpoint=checkpoint,
            db_url=self.db_url,
            canonical_schema=canonical_schema,
        )
        spooler = SpillBuffer(
            writer=writer,
            threshold=self.spill_threshold,
            spool_dir=dest_dir / "_spool" / schema_id,
        )
        normalizer = RowNormalizer(schema_mappings, column_profiles=column_profiles)
        total_estimated_rows = sum(self._estimate_block_rows(block) for block in blocks)
        processed_rows = writer.total_rows
        next_progress_emit = processed_rows + self.progress_granularity
        progress_path = dest_dir / f"{writer.slug}.materialize"
        processed_blocks = 0
        start_time = time.perf_counter()
        # Local dedup within schema plus optional global dedup across schemas.
        seen_lines: set[tuple[str, int]] = set()
        for idx, block in enumerate(blocks):
            if idx < start_block:
                processed_blocks += 1
                continue
            for line_number, row in iter_block_rows(block, self.encoding, self.errors):
                key = (str(block.file_path), line_number)
                if key in seen_lines:
                    continue
                if global_seen_lines is not None and key in global_seen_lines:
                    continue
                seen_lines.add(key)
                if global_seen_lines is not None:
                    global_seen_lines.add(key)
                normalized_row = normalizer.normalize(row, schema, source_path=block.file_path)
                spooler.push(normalized_row)
                processed_rows += 1
                if (
                    progress_callback
                    and (processed_rows >= next_progress_emit or processed_rows == total_estimated_rows)
                ):
                    self._emit_progress_event(
                        progress_callback,
                        progress_path,
                        processed_rows,
                        total_estimated_rows,
                        start_time,
                        schema_id,
                        schema.name,
                        spooler.telemetry.rows_spilled,
                    )
                    next_progress_emit = processed_rows + self.progress_granularity
            processed_blocks += 1
            spooler.flush()
            snapshot = writer.snapshot(next_block=idx + 1)
            self.checkpoints.update(schema_id, snapshot)
        spooler.close()
        writer.close()
        duration = time.perf_counter() - start_time
        rows = writer.total_rows
        rows_per_second = rows / duration if duration else float(rows)
        self.checkpoints.clear(schema_id)
        if progress_callback:
            self._emit_progress_event(
                progress_callback,
                progress_path,
                rows,
                max(rows, total_estimated_rows),
                start_time,
                schema_id,
                schema.name,
                spooler.telemetry.rows_spilled,
            )
        summary = JobSummary(
            schema_id=schema_id,
            schema_name=schema.name,
            blocks_processed=processed_blocks,
            rows_written=rows,
            rows_per_second=rows_per_second,
            output_files=writer.output_files,
            duration_seconds=duration,
            validation=writer.validation_summary,
            spill_metrics=spooler.telemetry,
        )
        self._emit_telemetry(summary)
        return summary

    @staticmethod
    def _estimate_block_rows(block: FileBlock) -> int:
        if block.end_line < block.start_line:
            return 0
        return (block.end_line - block.start_line) + 1

    def _emit_progress_event(
        self,
        callback: Callable[[FileProgress], None],
        file_path: Path,
        processed_rows: int,
        total_rows: int,
        start_time: float,
        schema_id: str,
        schema_name: str,
        spill_rows: int,
    ) -> None:
        # total_rows for materialization is often a rough estimate; to avoid
        # confusing "8M/57K" style progress, we only trust it when it is
        # explicitly positive and relatively small. Otherwise we drop it and
        # compute ETA based solely on processed_rows.
        eta = None
        rows_per_second = None
        effective_total = total_rows if (total_rows and total_rows > 0 and total_rows <= 10_000_000) else 0

        if processed_rows > 0:
            elapsed = time.perf_counter() - start_time
            if elapsed > 0:
                rate = processed_rows / elapsed
                if rate > 0:
                    rows_per_second = rate
                    if effective_total:
                        remaining = max(effective_total - processed_rows, 0)
                        eta = remaining / rate
        progress = FileProgress(
            file_path=file_path,
            processed_rows=processed_rows,
            total_rows=effective_total or 0,
            current_phase="materialize",
            eta_seconds=eta,
            schema_id=schema_id,
            schema_name=schema_name or None,
            rows_per_second=rows_per_second,
            spill_rows=spill_rows,
        )
        callback(progress)

    def _emit_telemetry(self, summary: JobSummary) -> None:
        if not self.telemetry_log:
            return
        payload = {
            "schema_id": summary.schema_id,
            "schema_name": summary.schema_name,
            "rows_written": summary.rows_written,
            "duration_seconds": summary.duration_seconds,
            "rows_per_second": summary.rows_per_second,
            "validation": summary.validation.__dict__,
            "spill": summary.spill_metrics.__dict__,
            "timestamp": time.time(),
        }
        self.telemetry_log.parent.mkdir(parents=True, exist_ok=True)
        with self.telemetry_log.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")


def build_schema_writer(
    *,
    format_name: str,
    schema: SchemaDefinition,
    dest_dir: Path,
    chunk_rows: int,
    encoding: str,
    errors: str,
    checkpoint: Optional[Dict[str, object]] = None,
    db_url: Optional[str] = None,
    canonical_schema: CanonicalSchema | None = None,
) -> BaseSchemaWriter:
    if format_name == "parquet":
        return ParquetSchemaWriter(
            schema,
            dest_dir,
            chunk_rows=chunk_rows,
            encoding=encoding,
            errors=errors,
            checkpoint=checkpoint,
            canonical_schema=canonical_schema,
        )
    if format_name == "database":
        return DatabaseSchemaWriter(
            schema,
            dest_dir,
            chunk_rows=chunk_rows,
            encoding=encoding,
            errors=errors,
            checkpoint=checkpoint,
            db_url=db_url,
            canonical_schema=canonical_schema,
        )
    return CSVSchemaWriter(
        schema,
        dest_dir,
        chunk_rows=chunk_rows,
        encoding=encoding,
        errors=errors,
        checkpoint=checkpoint,
        canonical_schema=canonical_schema,
    )


class CanonicalValidator:
    """Per-row validator that enforces canonical contract requirements."""

    def __init__(self, schema: SchemaDefinition, canonical_schema: CanonicalSchema | None) -> None:
        self._schema = schema
        self._canonical_schema = canonical_schema
        self._column_index = self._build_index(schema)
        self.missing_required = 0
        self.type_mismatches = 0

    def validate(self, values: Sequence[str]) -> None:
        if self._canonical_schema is None:
            return
        width = len(values)
        for spec in self._canonical_schema.columns:
            slug = slugify(spec.name)
            if not slug:
                continue
            column_index = self._column_index.get(slug)
            value = values[column_index] if column_index is not None and column_index < width else ""
            if not value.strip():
                if spec.required and not spec.allow_null:
                    self.missing_required += 1
                continue
            if spec.allowed_values and value not in spec.allowed_values:
                self.type_mismatches += 1
                continue
            if not self._value_matches_type(spec, value):
                self.type_mismatches += 1

    def _build_index(self, schema: SchemaDefinition) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        for column in schema.columns:
            slug = slugify(column.normalized_name or column.raw_name or f"column_{column.index + 1}")
            if slug and slug not in mapping:
                mapping[slug] = column.index
        return mapping

    def _value_matches_type(self, spec, value: str) -> bool:
        data_type = (spec.data_type or "string").lower()
        try:
            if data_type in {"string"}:
                return True
            if data_type in {"int", "integer"}:
                numeric_value = int(value)
                return self._check_bounds(spec, float(numeric_value))
            if data_type in {"float", "double", "decimal", "number"}:
                numeric_value = float(value)
                return self._check_bounds(spec, numeric_value)
            if data_type in {"bool", "boolean"}:
                normalized = value.strip().lower()
                return normalized in {"true", "false", "1", "0", "yes", "no"}
            if data_type == "date":
                date.fromisoformat(value)
                return True
            if data_type == "datetime":
                datetime.fromisoformat(value)
                return True
            if data_type == "json":
                json.loads(value)
                return True
        except Exception:
            return False
        return True

    @staticmethod
    def _check_bounds(spec, numeric_value: float) -> bool:
        if spec.min_value is not None and numeric_value < spec.min_value:
            return False
        if spec.max_value is not None and numeric_value > spec.max_value:
            return False
        return True


class ValidationTracker:
    """Normalizes rows to schema width and records validation stats."""

    def __init__(self, expected_columns: int, *, canonical_validator: CanonicalValidator | None = None) -> None:
        self.expected_columns = max(1, expected_columns)
        self.total_rows = 0
        self.short_rows = 0
        self.long_rows = 0
        self.empty_rows = 0
        self._canonical_validator = canonical_validator

    def normalize(self, values: Sequence[str], *, observed_length: Optional[int] = None) -> List[str]:
        normalized = list(values)
        if not any(value.strip() for value in normalized):
            self.empty_rows += 1
        length_hint = observed_length if observed_length is not None else len(normalized)
        if length_hint < self.expected_columns:
            self.short_rows += 1
            if len(normalized) < self.expected_columns:
                normalized.extend([""] * (self.expected_columns - len(normalized)))
            elif len(normalized) > self.expected_columns:
                normalized = normalized[: self.expected_columns]
        elif length_hint > self.expected_columns:
            self.long_rows += 1
            normalized = normalized[: self.expected_columns]
        else:
            if len(normalized) < self.expected_columns:
                normalized.extend([""] * (self.expected_columns - len(normalized)))
            elif len(normalized) > self.expected_columns:
                normalized = normalized[: self.expected_columns]
        if self._canonical_validator is not None:
            self._canonical_validator.validate(normalized)
        self.total_rows += 1
        return normalized

    def summary(self) -> ValidationSummary:
        missing_required = self._canonical_validator.missing_required if self._canonical_validator else 0
        type_mismatches = self._canonical_validator.type_mismatches if self._canonical_validator else 0
        return ValidationSummary(
            total_rows=self.total_rows,
            short_rows=self.short_rows,
            long_rows=self.long_rows,
            empty_rows=self.empty_rows,
            missing_required=missing_required,
            type_mismatches=type_mismatches,
        )


class BaseSchemaWriter(ABC):
    """Shared chunk/resume logic for different output formats."""

    def __init__(
        self,
        schema: SchemaDefinition,
        dest_dir: Path,
        *,
        chunk_rows: int,
        encoding: str,
        errors: str,
        checkpoint: Optional[Dict[str, object]] = None,
        canonical_schema: CanonicalSchema | None = None,
    ) -> None:
        self.schema = schema
        self.dest_dir = dest_dir
        self.chunk_rows = chunk_rows
        self.encoding = encoding
        self.errors = errors
        self.header = [
            col.normalized_name or col.raw_name or f"column_{col.index + 1}"
            for col in schema.columns
        ]
        if not self.header:
            self.header = ["column_1"]
        self.slug = slugify(schema.name or f"schema_{schema.id}")
        self.chunk_index = int(checkpoint.get("chunk_index", 0)) if checkpoint else 0
        self.rows_in_chunk = int(checkpoint.get("rows_in_chunk", 0)) if checkpoint else 0
        self.total_rows = int(checkpoint.get("total_rows", 0)) if checkpoint else 0
        self.output_files: List[str] = list(checkpoint.get("output_files", [])) if checkpoint else []
        self._handle: Optional[TextIO] = None
        canonical_validator = (
            CanonicalValidator(schema, canonical_schema)
            if canonical_schema is not None
            else None
        )
        self.validation = ValidationTracker(
            len(self.header),
            canonical_validator=canonical_validator,
        )
        if self.rows_in_chunk > 0:
            self._open_current(append=True)
        else:
            self._start_new_chunk()

    def write(self, values: Sequence[str], *, observed_length: Optional[int] = None) -> None:
        normalized = self.validation.normalize(values, observed_length=observed_length)
        if self.rows_in_chunk >= self.chunk_rows:
            self.chunk_index += 1
            self._start_new_chunk()
        self._write_row(normalized)
        self.rows_in_chunk += 1
        self.total_rows += 1

    def snapshot(self, next_block: int) -> Dict[str, object]:
        return {
            "next_block": next_block,
            "chunk_index": self.chunk_index,
            "rows_in_chunk": self.rows_in_chunk,
            "total_rows": self.total_rows,
            "output_files": self.output_files,
        }

    def close(self) -> None:
        self._before_close()
        if self._handle:
            self._handle.close()
            self._handle = None

    @property
    def validation_summary(self) -> ValidationSummary:
        return self.validation.summary()

    def _start_new_chunk(self) -> None:
        self.close()
        path = self._path_for_chunk(self.chunk_index)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._open_stream(path, append=False)
        if path.as_posix() not in self.output_files:
            self.output_files.append(path.as_posix())
        self.rows_in_chunk = 0

    def _open_current(self, *, append: bool) -> None:
        path = self._path_for_chunk(self.chunk_index)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._open_stream(path, append=append)
        if path.as_posix() not in self.output_files:
            self.output_files.append(path.as_posix())

    def _open_stream(self, path: Path, *, append: bool) -> None:
        mode = "a" if append else "w"
        # Always write UTF-8 for output files so tools can read them reliably.
        self._handle = path.open(mode, newline="", encoding="utf-8", errors=self.errors)
        self._after_open(append)

    def _path_for_chunk(self, chunk_index: int) -> Path:
        return self.dest_dir / f"{self.slug}_{chunk_index:03}.{self.file_extension()}"

    @abstractmethod
    def file_extension(self) -> str:  # pragma: no cover - trivial
        ...

    @abstractmethod
    def _after_open(self, append: bool) -> None:
        ...

    @abstractmethod
    def _write_row(self, values: Sequence[str]) -> None:
        ...

    def _before_close(self) -> None:  # pragma: no cover - optional override
        pass


class CSVSchemaWriter(BaseSchemaWriter):
    def __init__(self, *args, **kwargs) -> None:
        self._csv_writer: Optional[csv.writer] = None
        super().__init__(*args, **kwargs)

    def file_extension(self) -> str:
        return "csv"

    def _after_open(self, append: bool) -> None:
        assert self._handle is not None
        self._csv_writer = csv.writer(self._handle)
        if not append:
            self._csv_writer.writerow(self.header)

    def _write_row(self, values: Sequence[str]) -> None:
        assert self._csv_writer is not None
        self._csv_writer.writerow(values)


class ParquetSchemaWriter(BaseSchemaWriter):
    FLUSH_ROWS = 2048

    def __init__(self, *args, **kwargs) -> None:
        if pa is None or pq is None:  # pragma: no cover - guarded by dependency
            raise RuntimeError(
                "pyarrow is required for parquet writers. Install the 'pyarrow' dependency."
            )
        checkpoint = kwargs.get("checkpoint")
        if checkpoint:
            resumed_rows = int(checkpoint.get("rows_in_chunk", 0) or 0)
            if resumed_rows:
                updated = dict(checkpoint)
                updated["chunk_index"] = int(updated.get("chunk_index", 0)) + 1
                updated["rows_in_chunk"] = 0
                kwargs["checkpoint"] = updated
        self._buffer: List[List[str]] = []
        self._current_path: Optional[Path] = None
        self._arrow_schema = None
        self._parquet_writer: Optional[Any] = None
        super().__init__(*args, **kwargs)

    def file_extension(self) -> str:
        return "parquet"

    def _open_stream(self, path: Path, *, append: bool) -> None:  # type: ignore[override]
        self._current_path = path
        self._buffer = []
        self._parquet_writer = None
        self._handle = None
        self._after_open(append)

    def _after_open(self, append: bool) -> None:
        if self._arrow_schema is None:
            self._arrow_schema = pa.schema([(name, pa.string()) for name in self.header])
        if self._current_path is None:
            raise RuntimeError("Parquet writer missing target path during open")
        self._parquet_writer = pq.ParquetWriter(self._current_path, self._arrow_schema)

    def _write_row(self, values: Sequence[str]) -> None:
        self._buffer.append(list(values))
        if len(self._buffer) >= self.FLUSH_ROWS:
            self._flush_buffer()

    def _before_close(self) -> None:
        self._flush_buffer()
        if self._parquet_writer is not None:
            self._parquet_writer.close()
            self._parquet_writer = None
        self._current_path = None

    def _flush_buffer(self) -> None:
        if not self._buffer or not self._parquet_writer:
            return
        columns = {
            name: [row[idx] for row in self._buffer]
            for idx, name in enumerate(self.header)
        }
        table = pa.table(columns, schema=self._arrow_schema)
        self._parquet_writer.write_table(table)
        self._buffer.clear()


class DatabaseSchemaWriter(BaseSchemaWriter):
    def __init__(self, *args, db_url: Optional[str], **kwargs) -> None:
        if not db_url:
            raise ValueError("Database writer requires --db-url (e.g., sqlite:///path/to.db)")
        self._db_path = resolve_sqlite_path(db_url)
        self._conn: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None
        self._insert_sql: Optional[str] = None
        self._row_index = 0
        super().__init__(*args, **kwargs)

    def file_extension(self) -> str:
        return "sqlite"

    def _open_stream(self, path: Path, *, append: bool) -> None:  # type: ignore[override]
        self._conn = sqlite3.connect(self._db_path)
        self._conn.row_factory = None
        self._cursor = self._conn.cursor()
        self._ensure_table()
        self._cursor.execute("BEGIN")
        self._handle = None
        self._after_open(append)

    def _after_open(self, append: bool) -> None:
        columns = ["chunk_index", "row_in_chunk"] + self.header
        placeholders = ", ".join("?" for _ in columns)
        quoted = ", ".join(f'"{name}"' for name in columns)
        self._insert_sql = f'INSERT INTO "{self.slug}" ({quoted}) VALUES ({placeholders})'
        self._row_index = self.rows_in_chunk

    def _write_row(self, values: Sequence[str]) -> None:
        assert self._cursor is not None and self._insert_sql is not None
        payload = (self.chunk_index, self._row_index, *values)
        self._cursor.execute(self._insert_sql, payload)
        self._row_index += 1

    def _before_close(self) -> None:
        if self._cursor and self._conn:
            self._conn.commit()
            self._cursor.close()
            self._conn.close()
            self._cursor = None
            self._conn = None

    def _ensure_table(self) -> None:
        assert self._conn is not None
        columns = ", ".join(f'"{name}" TEXT' for name in self.header)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS "{self.slug}" (
            chunk_index INTEGER,
            row_in_chunk INTEGER,
            {columns}
        )
        """
        self._conn.execute(ddl)


def resolve_sqlite_path(db_url: str) -> Path:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError("Only sqlite:/// URLs are supported for database writers")
    raw_path = db_url[len(prefix) :]
    path = Path(raw_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class SpillBuffer:
    """Back-pressure buffer that spills to temp JSONL files when saturated."""

    def __init__(self, *, writer: BaseSchemaWriter, threshold: int, spool_dir: Path) -> None:
        self.writer = writer
        self.threshold = max(1, threshold)
        self.spool_dir = spool_dir
        self.buffer: List[NormalizedRow] = []
        self.telemetry = SpillMetrics()

    def push(self, row: NormalizedRow) -> None:
        self.buffer.append(row)
        self.telemetry.max_buffer_rows = max(self.telemetry.max_buffer_rows, len(self.buffer))
        if len(self.buffer) >= self.threshold:
            self._spill()

    def flush(self) -> None:
        if not self.buffer:
            return
        for row in self.buffer:
            self.writer.write(row.values, observed_length=row.observed_length)
        self.buffer.clear()

    def close(self) -> None:
        self.flush()

    def _spill(self) -> None:
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        spill_path = self.spool_dir / f"spill_{uuid4().hex}.jsonl"
        with spill_path.open("w", encoding="utf-8") as handle:
            for row in self.buffer:
                payload = {"values": row.values, "observed_length": row.observed_length}
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self.telemetry.spills += 1
        self.telemetry.rows_spilled += len(self.buffer)
        self.telemetry.bytes_spilled += spill_path.stat().st_size
        self.buffer.clear()
        self._drain_spill(spill_path)

    def _drain_spill(self, path: Path) -> None:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                data = json.loads(line)
                if isinstance(data, dict):
                    values = data.get("values", [])
                    observed_length = int(data.get("observed_length", len(values)))
                else:
                    values = list(data)
                    observed_length = len(values)
                self.writer.write(values, observed_length=observed_length)
        path.unlink(missing_ok=True)


class CheckpointStore:
    """Thread-safe checkpoint storage (JSON)."""

    def __init__(self, path: Optional[Path]) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._state = self._load()

    def get(self, schema_id: str) -> Dict[str, object]:
        return dict(self._state.get(schema_id, {}))

    def update(self, schema_id: str, snapshot: Dict[str, object]) -> None:
        if not self.path:
            return
        with self._lock:
            self._state[schema_id] = snapshot
            self._persist()

    def clear(self, schema_id: str) -> None:
        if not self.path:
            return
        with self._lock:
            if schema_id in self._state:
                del self._state[schema_id]
                self._persist()

    def _load(self) -> Dict[str, Dict[str, object]]:
        if not self.path or not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except json.JSONDecodeError:
            return {}

    def _persist(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(self._state, handle, indent=2)


def iter_block_rows(block: FileBlock, encoding: str, errors: str) -> Iterable[Tuple[int, List[str]]]:
    delimiter = block.signature.delimiter or ","
    header_sample = (block.signature.header_sample or "").strip()
    skip_header = bool(header_sample and (block.block_id == 0 or block.start_line == 0))
    header_skipped = False
    with block.file_path.open("r", encoding=encoding, errors=errors) as handle:
        for line_number, line in enumerate(handle):
            if line_number < block.start_line:
                continue
            if line_number > block.end_line:
                break
            stripped = line.rstrip("\n\r")
            if skip_header and not header_skipped and stripped.strip() == header_sample:
                header_skipped = True
                continue
            values = [value.strip() for value in stripped.split(delimiter)]
            yield line_number, values


def slugify(value: str) -> str:
    safe = [ch.lower() if ch.isalnum() else "_" for ch in value.strip()]
    slug = "".join(safe) or "dataset"
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")
