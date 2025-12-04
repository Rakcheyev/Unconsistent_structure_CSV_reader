# Unconsistent Structure CSV Reader (Python Skeleton)

Цей репозиторій містить каркас інструменту для двофазного аналізу й нормалізації CSV/TSV файлів із непослідовною структурою. Поточна версія фокусується на Python-реалізації з чітким поділом на UI, Core Engine та Storage/Config рівні.

## Архітектурні блоки
- **UI** – WPF/WinUI-подібний досвід, зараз описаний як Python-заглушка з вимогами до візуальних сцен, прогрес-барів та workflow-driven UX.
- **Core Engine** – модулі `analysis`, `mapping`, `normalization`, `materialization`, які реалізують відповідні фази (Phase 1, Phase 1.5, Phase 2) і працюють паралельно по файлах/схемах.
- **Storage & Config** – JSON/SQLite-шар для схем, мапінгу блоків, статистики колонок і словників синонімів.
- **Docs** – детальні вимоги до UX, алгоритмів семплінгу та дизайн-рішення загалом.

## Поточний статус
- `src/common/models.py` тепер описує повний набір сутностей (блоки, профайли, canonical контракти) + JSON/SQLite серіалізацію без втрати артефактів.
- Phase 1 має стрімінговий колонний профайлер (null %, HLL-lite уникальні, numeric/date min/max) із артефактами `mapping.column_profiles.json` та таблицею SQLite `column_profiles`.
- Phase 1.5 запускає графовий `HeaderClusterizer`, зберігає `mapping.header_clusters.json`, а `detect_offsets` генерує `schema_mapping` з confidence/offset метаданими.
- `NormalizationService` синхронізує схеми з `CanonicalSchemaRegistry` (фікстури в `storage/canonical_schemas.json`) і проставляє `canonical_schema_id`, типи та обов'язковість колонок.
- Phase 2 `MaterializationJobRunner` підтримує CSV/Parquet/SQLite writers, централізований `CheckpointRegistry` (JSON на `artifacts/checkpoints/<phase>/<job_id>.json`) із `--resume JOB_ID`, дві паралельні джоби, spill-to-temp буфер та ETA/`FileProgress` телеметрію.
- `CanonicalValidator` під час матеріалізації рахує `missing_required` + `type_mismatches`, доповнюючи класичні short/long row лічильники; усі метрики пишуться в `ValidationSummary`, JSONL та SQLite `job_metrics`.
- CLI `uscsv` охоплює Import → Analyze → Review → Normalize → Materialize, має бенчмарк/telemetry режими, прапорці `--sqlite-db`, `--telemetry-log`, `--writer-format`, `--spill-threshold`, `--db-url`.
- Запущено єдиний Job State Machine: `uscsv materialize` приймає `--job-id` (авто-генерується), `JobStateMachine` фіксує переходи `PENDING → MATERIALIZING → VALIDATING → DONE/FAILED`, а SQLite отримав таблиці `job_status` + `job_events` з доступом через `storage.fetch_job_status` (див. `docs/architecture/job_lifecycle.md`).
- Документація (workflow, UI/UX, sampling, materialization план) та `TASKS.md` синхронізовані зі Sprint 1/2 прогресом; `AGENTS.md` фіксує правила взаємодії агентів.
- Bootstrap-скрипти (`scripts/bootstrap_env.*`) + smoke-тести (`tests/`) забезпечують швидке стартове середовище, `pyproject.toml` конфігурує `ruff`, `mypy`, `pytest` для Python 3.12.

## Наступні кроки
1. UX tagging canonical schemas у Phase 1.5: dropdown + CLI fallback, audit entries та масове застосування namespace/schema-id.
2. Validation roadmap: розширити перевірки типів (enum/domain правила, діапазони) та surfaced counters у CLI/GUI dashboards.
3. Phase 2 writers: включити DB bulk loaders / Arrow Datasets, покрити recovery/regression сценаріями й telemetry порівняннями.
4. DX/GUI Sprint 3: DearPyGui/WinUI макети для schema cards, merge dialog, progress history та agents-driven workflows.

## Hardware Profiles & Setup

| Profile        | Target hardware                 | block_size | max_parallel_files | sample_values_cap |
| -------------- | ------------------------------- | ---------- | ------------------ | ----------------- |
| `low_memory`   | 2-core CPU, ≤2 GB RAM, HDD      | 1 000      | 1                  | 24                |
| `workstation`  | 8-core CPU, ≥16 GB RAM, SSD     | 10 000     | 4                  | 64                |

Параметри лежать у `config/defaults.json` і можуть бути підлаштовані під конкретні кластери/ноутбуки. Під час ранньої експлуатації рекомендується починати з `low_memory`, щоб уникнути пікових викидів RAM/IO, а потім підвищувати паралельність.

## Fast Bootstrap

```powershell
PS> scripts/bootstrap_env.ps1 -Dev
```

```bash
$ ./scripts/bootstrap_env.sh --dev
```

Скрипти створюють `.venv`, оновлюють `pip` і ставлять поточний пакет (з dev-інструментами за потреби). Це гарантує однакове оточення навіть на версіях Windows Server або мінімальних Linux-боксах.


## GUI Status

The legacy DearPyGui desktop interface has been retired and is no longer shipped with the repository. All workflows run through the CLI job system described in `docs/workflows/cli.md`. If you still have the old `src/ui/uscsv_gui.py` locally, treat it as deprecated and avoid using it for new workstreams.

- Batch processing of all CSV/TSV files in the selected folder
- Output format selection (CSV, JSON)
- Memory and chunk size configuration
- Progress bar and log window for feedback
- Error handling and completion messages

### Troubleshooting

- If you see missing DLL or dependency errors, ensure all required Python packages are installed before packaging.
- For large datasets, increase memory cap and chunk size as needed.
- If the GUI does not launch, check that your graphics drivers support OpenGL (required by DearPyGui).


Після bootstrap доступний покроковий CLI (`uscsv`) для всього сценарію Import → Analyze → Review → Normalize → Materialize:

1. **Аналіз** — `uscsv analyze data/raw --profile low_memory --output mapping.json --progress-log artifacts/progress.jsonl --sqlite-db artifacts/storage.db`. Команда читає файли паралельно, стрімить прогрес у консоль + JSONL та одразу синхронізує SQLite, якщо передано `--sqlite-db`.
2. **Бенчмарк** — `uscsv benchmark data/raw --profile workstation --log artifacts/bench.jsonl` для порівняння throughput профілів/машин.
3. **Рев'ю / кластеризація** — `uscsv review mapping.json --output mapping.review.json --synonyms storage/synonyms.json --sqlite-db artifacts/storage.db` запускає heuristic clustering і синхронізує результати в SQLite.
4. **Нормалізація назв** — `uscsv normalize mapping.review.json --output mapping.normalized.json --sqlite-db artifacts/storage.db` застосовує словник синонімів.
5. **Матеріалізація (реальний writer)** — `uscsv materialize mapping.normalized.json --dest artifacts/output --checkpoint-dir artifacts/checkpoints --plan artifacts/materialization_plan.json --writer-format parquet --spill-threshold 20000 --telemetry-log artifacts/materialize_metrics.jsonl --sqlite-db artifacts/storage.db --db-url sqlite:///artifacts/output.db [--job-id JOB42]`. Команда створює chunked вихід (CSV, Parquet через PyArrow, або SQLite database) із централізованими checkpoint'ами (`artifacts/checkpoints/materialize/<job_id>.json`), не тримає більше двох активних writers, логуватиме short/long rows, spill events, throughput у SQLite (`job_metrics`) / JSONL та випромінює ETA/`FileProgress` події для UI; прапорець `--job-id` дає змогу спостерігати стан у `job_status`, а `--resume JOB42` відновлює незавершений job із того ж snapshot.


Для швидкого smoke-прогону існує `scripts/run_cli_smoke.ps1`, який запускає `uscsv analyze` на `tests/data/retail_small.csv`, а потім автоматично видаляє тимчасові `artifacts/cli_smoke*.{json,db}` незалежно від результату.

Прапорець `--include-samples` на кожному етапі додає в JSON обмежену кількість `sample_values` (корисно для дебагу, але збільшує розмір файлу).

## SQLite/Parquet persistence, словник синонімів і телеметрія

- Файл `storage/synonyms.json` містить просту мапу `NormalizedName -> [variants...]`. CLI автоматично підтягує його для команд `review` та `normalize`, але за потреби шлях можна перевизначити через `--synonyms`.
- Параметр `--sqlite-db` (опційний для analyze/review/normalize/materialize) створює/оновлює SQLite із таблицями `schemas`, `blocks`, `stats`, `synonyms`, `audit_log`, `job_metrics`, `job_progress_events`. `job_metrics` зберігає per-schema rows, rows/s, short/long rows, spill телеметрію, а `job_progress_events` — кожен `FileProgress` tick (processed rows, ETA, rows/s, spill count) для live/history UI панелей. Дані читаються через `storage.fetch_job_progress_events` і автоматично прунінгуються до 500 записів на схему (`storage.prune_progress_history` дає ручний контроль). `storage.initialize` веде таблицю `schema_migrations`, тож будь-який виклик CLI автоматично застосовує відсутні міграції без ручного SQL.
- `MappingConfig.to_dict()/from_dict()` (обгорнуті навколо `common.mapping_serialization`) дозволяють швидко конвертувати mapping у JSON-ready dict без копіювання `sample_values`, якщо прапорець `include_samples=False` (використовується CLI/storage для легких записів).
- `--telemetry-log` для `materialize` накопичує JSONL-рядки з throughput/validation/spill даними; це використовується UI історією job'ів без доступу до SQLite.
- `--writer-format` тепер обирає реальні writers: `csv`, `parquet` (PyArrow `.parquet` файли) або `database` (SQLite таблиця, вимагає `--db-url=sqlite:///...`). `--spill-threshold` дає змогу примусити spill-to-temp/back-pressure і переглядати, коли writer'и не встигають.

### SQLite upgrade path

1. (Опційно) створіть копію чинної БД: `Copy-Item artifacts/storage.db artifacts/storage.backup.db`.
2. Запустіть будь-яку команду `uscsv` із тим же шляхом (`--sqlite-db artifacts/storage.db`). Перед виконанням фази CLI викличе `storage.initialize`, що проганяє `_apply_migrations` і оновлює `schema_migrations`.
3. Перевірте версію міграцій за потреби: `python -m sqlite3 artifacts/storage.db "SELECT * FROM schema_migrations ORDER BY version;"`.
4. Команду можна повторювати безпечно — міграції ідемпотентні, тож додаткових кроків чи ручного SQL не потрібно.

## Smoke Tests

Папка `tests/` містить перші smoke-тести й фікстури:

- `tests/data/retail_small.csv` — крихітний датасет для перевірки пайплайнів без великого IO.
- `tests/test_config_profiles.py` — гарантує, що всі профілі мають необхідні поля й можуть бути прочитані в CI (<1 s).

Запуск: `python -m pytest tests -q` (після bootstrap).

## Tracking Progress

- Операційні плани зберігаються в `TODO.md`.
- Детальний журнал змін — у `CHANGELOG.md`.
