# TODO Plan (Resource-Constrained Strategy)

## Phase 0 — Baseline & Guardrails
- [ ] [Platform] Ship a lean `uv`/`pip-tools` lock (or requirements export) so reproducible installs work offline on 2-core/8 GB targets.

## Phase 1 — Streaming Analysis Engine
- [ ] [Core Analysis] Implement `LineCounter` and `BlockPlanner` from `docs/data_sampling.md` with memory caps (single block buffer <= 1 MB) and chunked file IO.
- [x] [Core Analysis] Add adaptive throttling: auto-reduce `max_parallel_files` when disk queue depth spikes (simple moving average of read latency).
- [ ] [Storage] Define `MappingConfig` serialization helpers (`to_dict`/`from_dict`) that avoid materializing full `sample_values` when not requested.
- [x] [Observability] Emit structured progress events (JSONL) with timestamps so UI/history panes can stream without storing entire result sets.
- [ ] [Perf QA] Micro-benchmark sampling on files 50 MB / 500 MB / 2 GB and record CPU, peak RSS, throughput.

## Phase 1.5 — Schema Mapping & Normalization Prep
- [ ] [Storage] Create SQLite schema migrations for `schemas`, `blocks`, `stats`, `synonyms`, ensuring indices fit in shared-cache mode (<50 MB).
- [ ] [Designer] Prepare wireframes for schema cards + merge dialog (lo-res PNG or ASCII mock) and document focus states for keyboard-only workflows.
- [ ] [PM] Define acceptance tests covering merge/split scenarios and edge cases (e.g., header missing, mixed delimiters).

## Phase 2 — Value Normalization & Materialization
- [ ] [Normalization] Implement column profiler with streaming aggregations (reservoir sampling for top-N, HyperLogLog-lite for unique count estimates) to stay under 256 MB RAM.
- [x] [Core Materialization] Build schema-specific job runner with back-pressure: queue max 2 active writers, spill to temp files if downstream slower than reader. _(Spill buffer + telemetry + resume shipped.)_
- [ ] [Storage] Provide pluggable writers (CSV chunker, Parquet optional) behind feature flags so low-resource deployments can disable heavy formats.
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
