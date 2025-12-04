# Job Lifecycle & State Machine

This document describes how backend jobs advance through the Import → Analyze → Review → Normalize → Materialize workflow and how their state is persisted for UI/agent consumers.

## State Model

Jobs now share a unified lifecycle with the following states:

| State | Purpose |
| ----- | ------- |
| `PENDING` | Job registered, awaiting work. |
| `ANALYZING` | Phase 1 analysis running. |
| `MAPPING` | Phase 1.5 review/normalization in progress. |
| `MATERIALIZING` | Phase 2 writers actively producing datasets. |
| `VALIDATING` | Canonical validator consolidating metrics/checksums. |
| `DONE` | Job finished successfully. |
| `FAILED` | Job aborted due to an unrecoverable error (detail captures the message). |
| `CANCELLED` | Job stopped on purpose (user/system initiated). |

Transitions must move forward through the pipeline. `FAILED` and `CANCELLED` can be triggered from any non-terminal state, and terminal states reject additional transitions.

## SQLite Persistence

Two new tables capture lifecycle data:

- `job_status(job_id PRIMARY KEY, state, detail, last_error, metadata_json, created_at, updated_at)` — a single row per job with the current state and optional metadata (CLI command, profile, etc.).
- `job_events(id, job_id, state, detail, created_at)` — append-only log of every transition.

Migrations automatically create both tables (schema migration version `4`).

## Runtime Integration

- `src/core/jobs/state_machine.py` implements `JobStateMachine`, a thread-safe helper that enforces transitions and writes to SQLite through `storage.upsert_job_status` + `storage.record_job_event`.
- `MaterializationJobRunner` receives an optional tracker and emits `MATERIALIZING → VALIDATING → DONE` transitions. Any unhandled exception marks the job as `FAILED` with the stack message.
- `uscsv materialize` exposes `--job-id` (auto-generated when omitted) and passes that identifier to the job runner + tracker. The CLI prints the `job_id` so operators/agents can poll status later.
- UI/agents can call `storage.fetch_job_status(db_path, job_id)` to retrieve the current `JobStatusRecord` snapshot and drive live dashboards.

Future phases (analysis/review/normalization orchestration) can reuse the same tracker so that `ANALYZING`/`MAPPING` transitions are visible without adding new storage primitives.

## Checkpoint Registry & Resume Flow

- `core.jobs.checkpoints.CheckpointRegistry` is the single source of truth for storing per-job/per-phase progress. Payloads are JSON blobs under `artifacts/checkpoints/<phase>/<job_id>.json` and include snapshot metadata (`next_block`, writer chunk info, timestamps).
- Materialization emits checkpoint updates whenever a block/snapshot completes and clears the record when a schema finishes successfully. Failures leave the snapshot intact for resume.
- CLI gains `--checkpoint-dir` (override storage root) та `--resume <job_id>` (повторне використання snapshot + lifecycle row), тож агенти можуть рестартити job'и без ручного копіювання файлів.
- Storage keeps `job_status` / `job_events` in sync with checkpoint transitions, letting observers correlate SQLite status with filesystem checkpoints.
