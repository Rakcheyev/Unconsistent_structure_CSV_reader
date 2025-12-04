# Resource Manager (Backend-03)

Backend-Hardening item 03 introduces a lightweight `ResourceManager` that keeps all long-running agents within the hardware budgets defined by the active runtime profile.

## Responsibilities

1. **Central Budgets**
   - Track in-use memory, disk spill, and worker "slots".
   - Prevent oversubscription by denying new reservations once a limit is reached.
   - Keep limits configurable per profile (`profile.resource_limits`).
2. **Temp Directory Authority**
   - Own the `temp_dir` tree (defaults to `artifacts/tmp`).
   - Issue stable scratch subfolders per job/phase (`scratch_dir(job_id, "materialize", schema_id)`).
   - Provide `cleanup(job_id)` helpers once the job finishes.
3. **Worker Planning**
   - Convert user/CLI requested worker counts into safe values (`plan_workers(requested)`), so a profile with only 2 worker slots never spawns more threads/executors than allowed.
4. **Context-managed Reservations**
   - Expose `reserve(memory_mb=..., disk_mb=..., workers=...)` returning a context manager/lease that automatically releases usage counters.
   - Raise `ResourceLimitExceeded` with actionable hints when budgets are exhausted.

## Integration Points

- CLI instantiates one `ResourceManager` from `RuntimeConfig` and passes it to Phase 1/Phase 2 runners.
- Materialization spool paths are rooted at `scratch_dir(job_id, "materialize", schema_slug)`.
- Future phases (analysis/review) will reuse the same API to coordinate file readers, block planners, and spill buffers.

## Configuration Surface

```jsonc
"profiles": {
  "low_memory": {
    ...,
    "resource_limits": {
      "memory_mb": 1024,
      "spill_mb": 2048,
      "max_workers": 2,
      "temp_dir": "artifacts/tmp"
    }
  }
}
```

All fields are optional; omitting a limit keeps legacy behavior.
