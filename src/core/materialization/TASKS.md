# Materialization Module Tasks

1. **Job Planner** _(Status: ✅ done)_
   - Створює таск на кожну `SchemaDefinition` з переліком блоків (див. `planner.py`).
   - Забезпечує, що один job пише виключно у власні файли/таблиці.
2. **Block Reader** _(Status: ✅ done)_
   - Повторно відкриває файл, стрімить рядки в діапазоні `start_line..end_line`.
   - Використовує мапінг колонок і нормалізатори значень.
3. **Writers** _(Status: ✅ done)_
   - CSV, Parquet (PyArrow) і SQLite database writers доступні через `--writer-format` / `--db-url`.
   - CSV writer перевіряє, що навіть після resume/append порожні файли отримують заголовок, тож повторні запуски не продукують «голої» першої строки.
   - Подальші бекенди (Parquet via Arrow Datasets, cloud warehouses) можуть додаватися окремими задачами.
4. **Parallel Execution** _(Status: ⚙️ in progress)_
   - Обмеження максимум двох одночасних schema jobs реалізоване через `ThreadPoolExecutor`.
   - Spill-to-temp/back-pressure telemetry (`spill_count`, `rows_spilled`) + ETA/`FileProgress` події реалізовані; залишилось прокинути їх у UI/Storage history.
   - TODO: UI агент підписується на live події та читає історичні `job_progress_events` зі SQLite.
5. **Validation Hooks** _(Status: ✅ done)_
   - `ValidationSummary` (short/long/empty rows) виводиться у CLI, SQLite `job_metrics` і telemetry JSONL.
6. **Resume / Retry** _(Status: ✅ done — owner: core.materialization)_
   - `core.jobs.checkpoints.CheckpointRegistry` тепер єдине сховище (`artifacts/checkpoints/<phase>/<job_id>.json`), Materialization runner пише snapshot-и per schema, очищає їх після успіху й тримає сумісність із `JobStateMachine`.
   - CLI `uscsv materialize` має `--checkpoint-dir` для override кореня та `--resume JOB_ID` для restarts; `--job-id` стає ключем як для SQLite, так і для checkpointів.
   - Crash/resume тест (`tests/test_end_to_end_pipeline.py`) працює через registry й гарантує, що snapshot зберігається після збою та видаляється після повторного успіху; README/архдок/CHANGELOG оновлені.

7. **Progress Persistence** _(Status: ✅ done)_
   - Таблиця `job_progress_events` зберігає schema metadata, ETA, rows/s та spill rows, з автоматичним retention (500 записів/схему) + ручним `prune_progress_history`.
   - Runner через CLI комбінований callback записує всі `FileProgress` події; `storage.fetch_job_progress_events` дає UI/UX швидкий доступ до історії.
   - Docs (`docs/ui_ux.md`, `docs/workflow.md`) описують, як history панель читає ці дані.

8. **Writer Extensions** _(Status: ⚙️ in progress)_
   - Arrow Dataset writer (partitioned `.parquet` + hive-style директорії, див. `docs/materialization_writers_plan.md`).
   - Cloud warehouse targets (BigQuery/Snowflake) через copy jobs, конфіг за `--warehouse-target` (деталі в плані вище).
   - Наступні кроки: затвердити CLI контракт, додати feature flags та upload adapters перед реалізацією коду.

9. **End-to-End Validation** _(Status: ⚙️ in progress)_
   - Added `tests/test_end_to_end_pipeline.py`, який проганяє Import → Analyze → Review → Normalize → Materialize, симулює падіння через checkpoint hook, а потім перевіряє resume + history persistence.
   - Розширити сценарії на великі CSV (100k+ рядків) і мульти-schema пайплайни, додати performance assertions після стабілізації.
