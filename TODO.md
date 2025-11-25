# TODO Plan (Resource-Constrained Strategy)

## Phase 0 — Baseline & Guardrails
- [ ] [Platform] Ship a lean `uv`/`pip-tools` lock (or requirements export) so reproducible installs work offline on 2-core/8 GB targets.

## Phase 1 — Streaming Analysis Engine
- [x] [Core Analysis] Implement `LineCounter` and `BlockPlanner` from `docs/data_sampling.md` with memory caps (single block buffer <= 1 MB) and chunked file IO.
	- `core.analysis.line_counter` counts rows via 1 MB binary chunks, while `core.analysis.block_planner` plans/streams blocks with a configurable buffer cap.
	- `AnalysisEngine.analyze_file` now relies on the new services, keeping per-block memory bounded and avoiding bespoke helpers.
	- Covered by `tests/test_analysis_sampling.py` (line count + buffer limit cases).
- [x] [Core Analysis] Add adaptive throttling: auto-reduce `max_parallel_files` when disk queue depth spikes (simple moving average of read latency).
- [x] [Storage] Define `MappingConfig` serialization helpers (`to_dict`/`from_dict`) that avoid materializing full `sample_values` when not requested and keep types sourced from `src/common/models.py`.
	- Added `common.mapping_serialization` as the shared source of truth plus `MappingConfig.to_dict`/`from_dict` wrappers.
	- `storage.json_store` now delegates to the shared helpers, so CLI/storage stay aligned without duplicate type logic.
	- Tests in `tests/test_mapping_serialization.py` cover no-samples vs include-samples cases.
- [x] [Observability] Emit structured progress events (JSONL) with timestamps so UI/history panes can stream without storing entire result sets.
- [ ] [Perf QA] Micro-benchmark sampling on files 50 MB / 500 MB / 2 GB and record CPU, peak RSS, throughput in `docs/perf.md`.

## Phase 1.5 — Schema Mapping & Normalization Prep
- [x] [Storage] Ship SQLite schema migrations for `schemas`, `blocks`, `stats`, `synonyms`, ensuring indices fit in shared-cache mode (<50 MB).
	- `storage.initialize` now seeds `schema_migrations` (version, applied_at) and applies missing steps via `_apply_migrations`.
	- `schemas`: PK on `id`, index on `updated_at`; `blocks`: composite index `(schema_id, block_id)` plus covering `file_path` index.
	- `stats`: per-schema/per-column aggregates keyed to `schemas.id`; `synonyms`: canonical name + variant index to support case-insensitive lookups.
	- Verified by `tests/test_sqlite_store.py::test_migrations_apply_all_tables`.
- [x] [Storage] Wire CLI/bootstrap flow to call `storage.initialize()` before analyze/review steps and document the `--sqlite-db` rollback/upgrade story.
	- `src/ui/cli.py` now invokes `storage.init_sqlite` for any command that supplies `--sqlite-db`, so migrations run before Phase 1/2 units touch the database.
	- `README.md` section “SQLite upgrade path” walks through backup, rerun, and verification steps for existing installs.
- [ ] [Designer] Prepare wireframes for schema cards + merge dialog (lo-res PNG or ASCII mock) and document focus states for keyboard-only workflows.
- [ ] [PM] Define acceptance tests covering merge/split scenarios and edge cases (e.g., header missing, mixed delimiters).

## Phase 2 — Value Normalization & Materialization
- [ ] [Normalization] Implement column profiler with streaming aggregations (reservoir sampling for top-N, HyperLogLog-lite for unique count estimates) to stay under 256 MB RAM.
- [x] [Core Materialization] Build schema-specific job runner with back-pressure: queue max 2 active writers, spill to temp files if downstream slower than reader. _(Spill buffer + telemetry + resume shipped.)_
- [x] [Storage] Provide pluggable writers (CSV chunker, Parquet optional) behind feature flags so low-resource deployments can disable heavy formats. _(CSV, Parquet via PyArrow, SQLite DB writer delivered.)_
- [x] [Observability] Persist job metrics (rows/s, bytes/s, errors) in rolling SQLite window (max 500 entries) for lightweight history view. _(SQLite `job_metrics` table + CLI `--telemetry-log`.)_
- [ ] [QA] Draft recovery tests: simulate crash mid-block, resume using checkpoints; ensure no duplicate rows on restart.
- [ ] [QA] Author end-to-end regression suite that runs Import → Analyze → Review → Normalize → Materialize on synthetic 100k-row CSVs, captures checkpoints mid-run, resumes, and compares SQLite `job_metrics` + `job_progress_events` for monotonicity. _(Initial scenario covered by `tests/test_end_to_end_pipeline.py`; scale-test + 100k-row dataset still pending.)_

## Phase 3 — UI/UX & Interaction Layer
- [ ] [UI Dev] Implement virtualized list components (files, blocks, anomalies) with pagination window <= 200 rows in memory.
- [ ] [Designer] Finalize color semantics and contrast ratios for dark/light themes; document fallback palette for monochrome terminals.
- [ ] [UX Research] Run hallway tests on normalization table interactions, focusing on confidence-threshold microcopy for non-technical analysts.
- [ ] [Docs] Update `docs/ui_ux.md` with interaction diagrams and state machines for error panel + job runner.

## Phase 4 — Deployment & Continuous Quality
- [ ] [DevEx] Provide `makefile`/`invoke` tasks (or PowerShell equivalents) for lint, test, package, run-analysis to reduce cognitive load.
- [ ] [CI/CD] Configure GitHub Actions (or Azure DevOps) matrix with low-resource runners (2 vCPU) to ensure regressions are caught under tight budgets.
- [ ] [Security] Add checksum validation + sandboxed file reading (denylist double extensions) before analysis kicks off.
- [ ] [Telemetry] Optionally push anonymized metrics (counts only) to a central collector; keep feature behind opt-in flag for privacy-sensitive installs.
- [ ] [Release] Document versioning, changelog template, and packaging pipeline (PyPI or internal feed).
