# Materialization Writer Roadmap

## Arrow Dataset Writer
- **Goal**: produce partitioned Parquet datasets per schema using Arrow Datasets so downstream tools can query by partition without reparsing whole files.
- **Dependencies**: `pyarrow.dataset` (already available via PyArrow). Requires enabling dataset writer feature flag and optional filesystem backends (local disk first, S3-compatible targets later).
- **Output Layout**:
  - Directory structure: `<dest>/<schema_slug>/partition_col=value/part-XXXXX.parquet`.
  - Partition columns default to first normalized column, overridable via CLI (`--dataset-partition=<col>`).
  - Each part limited by `writer_chunk_rows`; dataset writer handles roll-over.
- **CLI/Config**:
  - `--writer-format=arrow-dataset` activates the backend.
  - Optional `--dataset-filesystem` URI (e.g., `file:///`, `s3://bucket/prefix`).
  - Fallbacks to local filesystem if URI omitted.
- **Telemetry**: reuse `FileProgress` with additional `partition_key` metadata (future extension) and log dataset manifest path per chunk.
- **Back-pressure**: share current `SpillBuffer`; for remote targets (S3) add async upload queue with bounded concurrency.
- **Resume Strategy**: checkpoint tracks dataset partition path + next file ordinal; on resume, create new dataset fragment to avoid rewriting existing Parquet parts.

## Cloud Warehouse Writers
- **Targets**: Snowflake and BigQuery as first-class destinations once credentials approved.
- **High-level Flow**:
  1. Write chunked Parquet (or gzip CSV) spill files locally using existing writers.
  2. Stage to cloud storage (`s3://` for Snowflake, `gs://` for BigQuery) with signed URLs or service accounts.
  3. Execute COPY/LOAD jobs via warehouse SDK/REST, tracking job IDs in telemetry.
- **Configuration**:
  - New CLI switch `--writer-format=warehouse` with `--warehouse-target=snowflake|bigquery`.
  - Connection info provided via env vars/JSON credentials referenced by `--warehouse-credentials` path.
  - Optional `--warehouse-stage` for external stage/bucket name.
- **Observability**:
  - `FileProgress` extended with `warehouse_job_id` field (only when applicable).
  - Store per-chunk load metrics inside SQLite `job_metrics` (rows loaded, job duration) plus dedicated `job_load_events` table (future).
- **Failure/Retry**:
  - If load job fails, runner retries configurable times before surfacing error; checkpoint stores last successful chunk ID to prevent duplicates.
  - Provide CLI `--warehouse-retry` limit.
- **Security**:
  - Keep credentials outside repo; only reference secure files.
  - Enforce masking when logging connection strings.

## Next Steps
1. Finalize CLI contract and config schema additions.
2. Add dataset writer implementation guarded by feature flag.
3. Integrate warehouse staging flow with pluggable upload adapters.
4. Expand tests/benchmarks to cover dataset + warehouse modes once dependencies confirmed.
