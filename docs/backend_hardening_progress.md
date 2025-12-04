# Backend Hardening Progress Log

_Last updated: 2025-12-04_

This document mirrors the current backend roadmap so it stays accessible even if power cuts interrupt the main session. Any change to the plan should append a short, timestamped note in the **Update History** section and adjust the checklist below.

## Active Checklist

- [x] Scan repo components — inventory job status, checkpoints, resources, versions, config validation, sandbox, metrics, API.
- [x] Implement Backend-01 — job state machine + `job_status`/`job_events` tables wired into runner/CLI/docs.
- [x] Implement Backend-02 — checkpoint registry + `--resume`, CLI/docs/changelog updates.
- [x] Implement Backend-03 — ResourceManager for RAM/disk/workers/temp dirs with docs/changelog.
- [ ] Implement Backend-04 — Versioned schema/mapping formats plus migrators, docs, changelog.
- [ ] Implement Backend-05 — Central config validation with error codes, docs, changelog.
- [ ] Implement Backend-06 — Filesystem sandbox for agents, docs, changelog.
- [ ] Implement Backend-07 — Metrics exporter interface + Prometheus exporter, docs, changelog.
- [ ] Implement Backend-08 — Programmatic API layer (`run_job`) and JSON contracts, docs, changelog.

## Update History

- _2025-12-04 00:00Z_ — Document created from current to-do list so progress can be tracked offline.
- _2025-12-04 10:05Z_ — Kicked off Backend-03 planning (ResourceManager design for RAM/disk/workers/temp dirs).
- _2025-12-04 12:45Z_ — Backend-03 finished: ResourceManager now enforces RAM/disk/worker budgets, owns temp dirs, exposes `reserve()/plan_workers()/scratch_dir()`, ships with docs/tests, and CLI/runners pass the singleton from runtime profiles.
