# Unconsistent Structure CSV Reader (Python Skeleton)

Цей репозиторій містить каркас інструменту для двофазного аналізу й нормалізації CSV/TSV файлів із непослідовною структурою. Поточна версія фокусується на Python-реалізації з чітким поділом на UI, Core Engine та Storage/Config рівні.

## Архітектурні блоки
- **UI** – WPF/WinUI-подібний досвід, зараз описаний як Python-заглушка з вимогами до візуальних сцен, прогрес-барів та workflow-driven UX.
- **Core Engine** – модулі `analysis`, `mapping`, `normalization`, `materialization`, які реалізують відповідні фази (Phase 1, Phase 1.5, Phase 2) і працюють паралельно по файлах/схемах.
- **Storage & Config** – JSON/SQLite-шар для схем, мапінгу блоків, статистики колонок і словників синонімів.
- **Docs** – детальні вимоги до UX, алгоритмів семплінгу та дизайн-рішення загалом.

## Поточний статус
- Створені pythonic `dataclasses` у `src/common/models.py`, які відповідають описаним C# моделям.
- Для кожного блоку додано `TASKS.md` з конкретними TODO і гранулярністю, достатньою для планування спринтів.
- Файл `AGENTS.md` містить правила координації між агендами/модулями.
- Додано `pyproject.toml` з мінімальними залежностями та конфігами `ruff`/`mypy` для суворого контролю якості на Python 3.11.
- Створено `config/defaults.json` із профілями для систем із низькими ресурсами та робочих станцій.
- Підготовлено bootstrap-скрипти (`scripts/bootstrap_env.ps1` / `.sh`) і smoke-тести в `tests/` із крихітними CSV-фікстурами.
- Фаза 1 проаналізована з адаптивним throttling, структурованими JSONL прогрес-логами та CLI-бенчмаркером.
- Phase 2 отримала перший реальний job runner: chunked CSV writers (відновлення з checkpoint, максимум дві паралельні схеми) і SQLite-аудит опційно з кожної CLI-команди.
- Додано валідаційні лічильники (short/long rows), spill-to-temp telemetry, writer-флаги (`--writer-format`, `--spill-threshold`) і SQLite `job_metrics` з rows/s.
- Реалізовано справжні Parquet (PyArrow) та SQLite database writers (через `--writer-format=parquet|database` + `--db-url`), а також ETA/`FileProgress` події для Phase 2, щоб UI міг малювати таймлайни матеріалізації.

## Наступні кроки
1. Додати lightweight колонний профайлер (reservoir sampling + HLL-lite) і метрики рядків/байтів у SQLite.
2. Підготувати Parquet/bulk-DB writer за тим самим інтерфейсом, включно з throttlingом.
3. Розширити `docs/ui_ux.md` моками для schema cards + merge dialog (див. також `docs/schema_review_brief.md`).
4. Автоматизувати прогін recovery tests: симулювати падіння в середині блока та відновлення з checkpoint.

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


## GUI Workflow (DearPyGui)

The project includes a graphical user interface (GUI) built with DearPyGui for easy batch processing and export of CSV/TSV files. The GUI allows you to:

- Select an input folder containing CSV/TSV files
- Select an output folder for results
- Choose output format (CSV, JSON)
- Set memory cap and chunk size
- View progress and logs during processing

### Running the GUI

To run the GUI from source:

```powershell
PS> python src/ui/uscsv_gui.py
```

### Packaging the GUI as an .exe

To create a standalone Windows executable:

```powershell
PS> pip install pyinstaller
PS> pyinstaller --onefile src/ui/uscsv_gui.py
```

The packaged `.exe` will be in the `dist` folder. Copy it to any Windows PC and run directly—no Python required.

### GUI Features

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
5. **Матеріалізація (реальний writer)** — `uscsv materialize mapping.normalized.json --dest artifacts/output --checkpoint artifacts/materialize_checkpoint.json --plan artifacts/materialization_plan.json --writer-format parquet --spill-threshold 20000 --telemetry-log artifacts/materialize_metrics.jsonl --sqlite-db artifacts/storage.db --db-url sqlite:///artifacts/output.db`. Команда створює chunked вихід (CSV, Parquet через PyArrow, або SQLite database) з відновленням по checkpoint, не тримає більше двох активних writers, логуватиме short/long rows, spill events, throughput у SQLite (`job_metrics`) / JSONL та випромінює ETA/`FileProgress` події для UI.


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
