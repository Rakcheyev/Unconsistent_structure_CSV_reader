# Workflow Overview

1. **Import Files**
   - Drag&Drop / file picker, швидкий підрахунок розміру та рядків.
   - Жодних рішень з боку користувача, лише валідація й підтвердження запуску аналізу.
2. **Analyze Schemas (Phase 1)**
   - Прохід файла блоками, побудова `FileBlock` + `SchemaSignature`.
   - Запуск у `ProcessPoolExecutor`, UI лише слухає прогрес і логі.
3. **Review & Edit Schemas (Phase 1.5)**
   - Карточки схем, merge/compare, нормалізація назв колонок, confidence-пороги.
4. **Normalize Values & Types**
   - Column profiler, виявлення аномалій, синоніми й очищення значень.
5. **Materialize Datasets (Phase 2)**
   - Chunked writers (CSV, реальний Parquet через PyArrow, SQLite database) з валідаційними лічильниками, spill-to-temp telemetry (`spills`, `rows_spilled`) і максимум двома активними job'ами.
   - Планувальник формує артефакт `materialization_plan.json`, job runner пише результати у `artifacts/output/`, а `--sqlite-db` додає запис у `job_metrics` (rows, rows/s, short/long rows).
   - `--telemetry-log` + ETA/`FileProgress` події дозволяють UI стрімити прогрес без доступу до файлової системи; Storage шар дублює ці ж події у таблицю `job_progress_events`, з якої UI читає через `fetch_job_progress_events`. Таблиця має автоматичний retention (500 записів на схему) із можливістю ручного `prune_progress_history`.
6. **Export & Audit**
   - Збереження JSON/SQLite конфігів, логів job'ів і статистики.

Кожен етап має власну панель прогресу, error log і можливість переглянути детальні метрики без pop-up діалогів.
