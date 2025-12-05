# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2025-12-01
## [0.3.0] - 2025-12-04
### Added
- Job lifecycle tracking: `JobStateMachine` emits transitions `PENDING → ANALYZING → MAPPING → MATERIALIZING → VALIDATING → DONE/FAILED/CANCELLED`, CLI now accepts `--job-id`, and SQLite gained `job_status`/`job_events` tables + `storage.fetch_job_status` accessor (documented in `docs/architecture/job_lifecycle.md`).
- Versioned mapping artifacts: `MappingConfig` writes `artifact_version`, header clusters carry per-cluster `version`, SQLite gained `artifact_metadata` plus canonical schema version columns so agents/UI can detect compatibility; legacy JSON auto-upgrades via the new serialization helpers.
- Config validation hardening: `ConfigDocument` centralizes parsing, validates required fields/paths/enums, raises structured `BackendError` codes (`CONFIG_ERROR`/`IO_ERROR`/etc.), and the CLI now surfaces those codes for operator/agent consumption.
- Filesystem sandbox: all CLI path parameters flow through `common.sandbox.Sandbox`, which resolves paths relative to the job root, enforces allowlists, blocks directory escapes, raises `BackendError` on violations, and now ships with regression tests.
- Central ResourceManager: profile-defined `memory_mb`/`spill_mb`/`max_workers` budgets are enforced through a shared `ResourceManager` that exposes `reserve()`, `plan_workers()`, and `scratch_dir()` helpers, owns `artifacts/tmp`, and is passed to Phase 1/2 runners + CLI; architecture note and regression tests cover the feature.
- Checkpoint registry + resume hardening: `core.jobs.CheckpointRegistry` stores per-job/per-phase JSON under `artifacts/checkpoints/<phase>/<job_id>.json`, `uscsv materialize` exposes `--checkpoint-dir` + `--resume JOB_ID`, the Phase 2 runner now requires job IDs for checkpointing, and the crash/resume regression test asserts snapshot persistence/cleanup.
- Streaming Phase 1 column profiler captures type distribution buckets, HyperLogLog-lite unique estimates, null counts, and numeric/date min-max per column. Results ship in `mapping.column_profiles`, dedicated JSON artifacts (`mapping.column_profiles.json`), and the new SQLite `column_profiles` table so Phase 1.5/2 modules can reuse the telemetry.
- Phase 1 header + column metadata (`header_occurrences`, `header_profiles`, `file_headers`, column profiles) are now recorded during `uscsv analyze`, persisted to JSON + SQLite, and exposed through `HeaderMetadata` helpers.
- Graph-driven `HeaderClusterizer` normalizes Cyrillic/Latin tokens, blends Levenshtein + n-gram Jaccard + token overlap scores, enforces type compatibility, and writes both `mapping.header_clusters` plus a standalone `mapping.header_clusters.json` artifact with canonical names, confidence, and review hints.
- Offset detection uses cluster + profile signals to emit richer `schema_mapping` entries (including confidence per mapping). `RowNormalizer` can now fall back to schema positions by comparing profiler buckets when canonical slugs do not match reviewed schema columns, keeping canonical rows aligned across drifting headers.
- CLI analyze saves the new artifacts, seeds SQLite with header + column telemetry, and regression suites now cover the profiler, column profile serialization, SQLite migrations, clusterizer behavior, offset detection, and RowNormalizer type-fallback logic.
- Adaptive throttling + structured progress logging for the analysis engine alongside a CLI `benchmark` command for throughput tracking.
- Optional SQLite persistence + audit logging toggled via `--sqlite-db` for analyze/review/normalize/materialize flows.
- Materialization job runner with chunked CSV writers, checkpoint/resume support, and CLI integration that limits active writers to two concurrent schemas.
- Tests covering the materialization runner to ensure chunk rotation and resume metadata stay consistent.
- Canonical schema fixture + registry loader (`storage/canonical_schemas.json` + `core.validation.load_canonical_registry`) power CLI/agents, and schemas now persist `canonical_schema_id` metadata for downstream validation.
- Validation counters (short/long rows), spill-to-temp telemetry, writer-format flags (CSV/Parquet stub/DB bulk), and per-schema telemetry JSONL logging.
- Canonical validator enforces required columns + type rules during materialization, surfaces `missing_required` / `type_mismatches` in `ValidationSummary`, and records the counts in SQLite job metrics + telemetry.
- SQLite `job_metrics` table with CLI helpers to persist rows/s + validation warnings for every materialization job.
- Real Parquet output via PyArrow and SQLite database writers hooked into `--writer-format` (with `--db-url`), plus ATA-friendly `FileProgress` events for Phase 2.
- UI/storage wiring for progress telemetry: `FileProgress` now carries schema metadata + rows/s, CLI persists every tick into SQLite `job_progress_events` alongside live console rendering.
- History API + retention for progress telemetry: `storage.record_progress_event` now prunes to 500 rows/schema, `fetch_job_progress_events`/`prune_progress_history` power the UI timeline, and `JobProgressEvent` dataclass documents the payload.
- End-to-end pipeline regression (`tests/test_end_to_end_pipeline.py`) covering Import → Analyze → Review → Normalize → Materialize with simulated crash/resume and history verification, plus a materialization writer roadmap doc for Arrow Dataset / warehouse backends.

### Changed
- Analyze/Batch workflows now include `schema_mapping` and profiler payloads by default, backfilling them before materialization when missing, and exporting artifacts for agents/UX review.
- Materialization runner keeps track of the original row width while reordering rows so short/long-row validation still reflects raw inputs despite canonical alignment.
- `uscsv materialize` now executes real writers and still emits a JSON plan for downstream automation.
- Documentation (`README.md`, `docs/workflow.md`, module `TASKS.md`, `docs/ui_ux.md`, `docs/materialization_writers_plan.md`, `docs/data_sampling.md`) updated to describe the profiler, header clustering flow, history APIs, benchmarks, and upcoming writer roadmap.

## [0.1.0] - 2025-11-25
### Added
- Initial Python 3.11 skeleton with workflow-first directory layout (UI, Core, Storage).
- Configuration baselines: `pyproject.toml` with Ruff/Mypy, profile-driven `config/defaults.json`, bootstrap scripts for Windows/Linux, and smoke tests with tiny CSV fixtures.
- Phase 1 analysis engine capable of sampling giant CSVs in resource-constrained environments plus a minimal `uscsv analyze` CLI entry point.
- Schema clustering service, synonym dictionary loader, normalization service, and placeholder materialization planner with accompanying CLI commands (`review`, `normalize`, `materialize`).

### Notes
- Future entries should follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) conventions.
