# Workflow Overview

1. **Import Files**
   - Drag&Drop / file picker, швидкий підрахунок розміру та рядків.
   - Жодних рішень з боку користувача, лише валідація й підтвердження запуску аналізу.
2. **Analyze Schemas (Phase 1)**
   - Прохід файла блоками, побудова `FileBlock` + `SchemaSignature` + `ColumnProfile` (HLL-lite унікальність, null %, top tokens, numeric/date min-max).
   - Колонковий профайлер стрімить результати у `mapping.column_profiles.json` та таблицю SQLite `column_profiles`, а також повертає агрегати до `HeaderMetadata` для подальших фаз.
   - Запуск у `ProcessPoolExecutor`, UI лише слухає прогрес і логі.
3. **Review & Edit Schemas (Phase 1.5)**
   - `HeaderClusterizer` запускається одразу після аналізу: будує граф подібності між усіма зафіксованими заголовками, розраховує `canonical_name`, `confidence_score`, `needs_review` та зберігає результат у `mapping.header_clusters` + SQLite + окремий артефакт `mapping.header_clusters.json`. Кожен кластер має поле `version`, а сам JSON включає `artifact_version`; SQLite дзеркалить ці значення у таблиці `artifact_metadata`, аби UI/агенти могли гарантувати сумісність рішень навіть після оновлення алгоритму.
   - Карточки схем, merge/compare, нормалізація назв колонок, confidence-пороги ґрунтуються на цих кластерах, type profile'ах і живих колонкових профілях (щоб виявити несумісність типів ще до review).
   - **Canonical schema contracts**: затверджені схеми описуються через `CanonicalSchema`/`CanonicalColumnSpec` у `src/common/models.py`, зберігаються у версійному реєстрі (`tests/data/schemas/approved_schemas.json`) і слугують єдиним джерелом правди для валідатора + UI. Кожна схема має `namespace`, `version`, ключові поля та allowed values, що полегшує узгодження різних джерел даних.
4. **Normalize Values & Types**
   - Режим нормалізації використовує профайлінг + `schema_mapping`: автоматично підтягує синтаксичні синоніми, перевіряє типову поведінку (null %, діапазони) та застосовує фолбек RowNormalizer'а до позицій стовпчиків, якщо `canonical_name` ще не підтверджений користувачем.
   - Після застосування синонімів `NormalizationService` синхронізує схеми з реєстром `CanonicalSchemaRegistry`: проставляє `canonical_schema_id`, `canonical_schema_version`, оновлює `SchemaColumn.data_type` і блокувальні прапори `required`/`allow_null` для наступних фаз. Ці namespace/version значення зберігаються і у SQLite `schemas`, тож рев'юери бачать точний контракт.
5. **Materialize Datasets (Phase 2)**
   - Chunked writers (CSV, реальний Parquet через PyArrow, SQLite database) з валідаційними лічильниками, spill-to-temp telemetry (`spills`, `rows_spilled`) і максимум двома активними job'ами.
   - Під час запису `CanonicalValidator` перевіряє кожен рядок проти затвердженого контракту: відсутні обов'язкові поля додають `missing_required`, несумісні типи/enum'и додають `type_mismatches`. Результати входять у `ValidationSummary` (CLI, telemetry, SQLite `job_metrics`).
   - Планувальник формує артефакт `materialization_plan.json`, job runner пише результати у `artifacts/output/`, а `--sqlite-db` додає запис у `job_metrics` (rows, rows/s, short/long rows) та оновлює `artifact_metadata` (ключі `mapping.artifact_version`, `header_clusters.version`) для прозорої перевірки новими агентами чи UI сесіями.
   - `--telemetry-log` + ETA/`FileProgress` події дозволяють UI стрімити прогрес без доступу до файлової системи; Storage шар дублює ці ж події у таблицю `job_progress_events`, з якої UI читає через `fetch_job_progress_events`. Таблиця має автоматичний retention (500 записів на схему) із можливістю ручного `prune_progress_history`.
6. **Export & Audit**
   - Збереження JSON/SQLite конфігів, логів job'ів і статистики.

Кожен етап має власну панель прогресу, error log і можливість переглянути детальні метрики без pop-up діалогів.
