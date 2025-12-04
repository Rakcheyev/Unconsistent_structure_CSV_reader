# Core Engine Umbrella Tasks

1. **API Contracts**
   - Визначити синхронні/асинхронні інтерфейси між analysis, mapping, normalization, materialization.
   - Узгодити payloadи прогресу та логів.
2. **Concurrency Guardrails**
   - Стандартизувати використання `SemaphoreSlim` (C#-аналог) / `asyncio.Semaphore` у Python.
   - Забезпечити, що UI отримує оновлення не рідше ніж раз на 500 мс.
3. **Error Taxonomy**
   - Категорії: IO, Parsing, SchemaMismatch, StorageFailure, UserAbort.
   - Всі модулі кидають кастомні виключення з кодами для UI.
4. **Testing Strategy**
   - Набір synthetic файлів для e2e (малий, середній, великий).
   - Benchmark сценарії для перевірки O(n) поведінки.
5. **Telemetry Bus**
   - Єдина подієва шина (наприклад, `asyncio.Queue`) для прогресу, логів, warnings.
6. **Configurable Parameters**
   - `block_size`, `min_gap_lines`, `max_parallel_files`, `confidence_thresholds`.
   - Значення зберігаються у Storage та підтягуються при старті job'ів.
7. **Header Clusterizer (Phase 1.5)**
   - Побудувати граф подібності на основі header metadata (occurrences, type profiles, sample values + колонкові профайли).
   - Визначати `canonical_name`, `confidence_score`, `needs_review`, серіалізувати у `mapping.header_clusters` + JSON артефакт і SQLite.
   - Status: done (owner: core.headers.cluster_builder, CLI analyze). Handoff: expose canonical suggestions + profiler conflicts у Review UI.

8. **Column Profiling Telemetry**
   - Стрімінговий профайлер (null %, HLL unique, min/max, top tokens) заповнює `ColumnProfile` об'єкти для кожного файлу/колонки.
   - Артефакти: `mapping.column_profiles.json`, SQLite `column_profiles`, in-memory злив у `HeaderMetadata` для кластеризатора/RowNormalizer.
   - Status: done (owner: core.analysis.column_profiler). Next: feed anomalies у Phase 1.5 cards та Normalize Values stage.

9. **Resource Manager (Backend-03)**
   - Єдиний `ResourceManager` модуль контролює бюджети RAM/disk/workers/temp-dir. API: `request(memory_mb=?, disk_mb=?, workers=?) -> lease` із автоматичним `release()`/context manager, `plan_workers(requested)` повертає безпечну кількість потоків/екзек’юторів, `scratch_dir(job_id, *segments)` створює підкаталоги у `artifacts/tmp` (override через конфіг), `cleanup(job_id)` видаляє тимчасові артефакти після job.
   - Бюджети походять з профіля (`profile.resource_limits`: `memory_mb`, `spill_mb`, `max_workers`, `temp_dir`). CLI підхоплює значення з `RuntimeConfig` й передає менеджеру у Phase 1/2 ранери.
   - Документація: додати архітектурну нотатку + README витяг, changelog фіксує появу модуля та нових ключів профілів.
   - Тести: unit-тести на резерв/звільнення та на те, що `plan_workers` поважає ліміти.
   - Status: done (owner: `core.resources.manager`, wired via CLI/runtime profiles; covered by `tests/test_resource_manager.py` + `tests/test_materialization_runner.py`, documented в `docs/architecture/resource_manager.md`).
