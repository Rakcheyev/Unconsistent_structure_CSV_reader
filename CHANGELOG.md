# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2025-12-01
## [0.3.0] - 2025-12-04
### Added
- Phase 1 header telemetry (`header_occurrences`, `header_profiles`, `file_headers`) now recorded during `uscsv analyze`, persisted to JSON + SQLite, and exposed via new `HeaderMetadata` helpers.
- Graph-driven `HeaderClusterizer` builds canonical names + confidence scores (with Cyrillic/Latin synonym awareness) and writes `mapping.header_clusters` for downstream review/materialization.
- Offset detection now emits `schema_mapping` entries off the header clusters, and `RowNormalizer` consumes them to realign per-file columns during materialization.
- Regression suites for clusterizer synonyms, schema mapping offsets, and RowNormalizer file-specific mappings.

### Changed
- Analyze/Batch workflows now include `schema_mapping` payloads by default and backfill them before materialization when missing.
- Materialization runner keeps track of the original row width while reordering rows so short/long-row validation still reflects raw inputs despite canonical alignment.

### Added
- Adaptive throttling + structured progress logging for the analysis engine alongside a CLI `benchmark` command for throughput tracking.
- Optional SQLite persistence + audit logging toggled via `--sqlite-db` for analyze/review/normalize/materialize flows.
- Materialization job runner with chunked CSV writers, checkpoint/resume support, and CLI integration that limits active writers to two concurrent schemas.
- Tests covering the materialization runner to ensure chunk rotation and resume metadata stay consistent.
- Validation counters (short/long rows), spill-to-temp telemetry, writer-format flags (CSV/Parquet stub/DB bulk), and per-schema telemetry JSONL logging.
- SQLite `job_metrics` table with CLI helpers to persist rows/s + validation warnings for every materialization job.
- Real Parquet output via PyArrow and SQLite database writers hooked into `--writer-format` (with `--db-url`), plus ATA-friendly `FileProgress` events for Phase 2.
- UI/storage wiring for progress telemetry: `FileProgress` now carries schema metadata + rows/s, CLI persists every tick into SQLite `job_progress_events` alongside live console rendering.
- History API + retention for progress telemetry: `storage.record_progress_event` now prunes to 500 rows/schema, `fetch_job_progress_events`/`prune_progress_history` power the UI timeline, and `JobProgressEvent` dataclass documents the payload.
- End-to-end pipeline regression (`tests/test_end_to_end_pipeline.py`) covering Import → Analyze → Review → Normalize → Materialize with simulated crash/resume and history verification, plus a materialization writer roadmap doc for Arrow Dataset / warehouse backends.

### Changed
- `uscsv materialize` now executes real writers and still emits a JSON plan for downstream automation.
- Documentation (`README.md`, `docs/workflow.md`, module `TASKS.md`, `docs/ui_ux.md`, `docs/materialization_writers_plan.md`) updated to describe the new runner, history APIs, benchmarks, and upcoming writer roadmap.

## [0.1.0] - 2025-11-25
### Added
- Initial Python 3.11 skeleton with workflow-first directory layout (UI, Core, Storage).
- Configuration baselines: `pyproject.toml` with Ruff/Mypy, profile-driven `config/defaults.json`, bootstrap scripts for Windows/Linux, and smoke tests with tiny CSV fixtures.
- Phase 1 analysis engine capable of sampling giant CSVs in resource-constrained environments plus a minimal `uscsv analyze` CLI entry point.
- Schema clustering service, synonym dictionary loader, normalization service, and placeholder materialization planner with accompanying CLI commands (`review`, `normalize`, `materialize`).

### Notes
- Future entries should follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) conventions.
