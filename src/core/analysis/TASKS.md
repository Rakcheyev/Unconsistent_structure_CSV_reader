# Analysis Module Tasks

1. **File Enumerator**
   - Збирає список файлів/папок, фільтрує за розширеннями `.csv|.tsv|.txt`.
   - Параметризує розмір блока (`block_size`) і мінімальний gap (`min_gap_lines`).
2. **Line Counting Service**
   - Буферизоване читання файлу з підтримкою кодувань (UTF-8, Windows-1251).
   - Видає `FileProgress` події кожні N тис. рядків.
3. **Sampling Planner**
   - Реалізує `build_sample_indices` та `to_block` (див. `docs/data_sampling.md`).
   - Забезпечує непересічність блоків і повторне використання кешованих семплів.
4. **Block Analyzer**
   - Стрімивно читає блок, визначає delimiter, header, `ColumnStats`.
   - Автоматично обмежує `sample_values` до 50 записів на колонку.
5. **Async Orchestrator**
   - Використовує `ProcessPoolExecutor` (або `multiprocessing.Pool`) для паралелі по файлах.
   - Отримує результати через `asyncio.as_completed`, передає у Storage.
6. **Diagnostics**
   - Лічильники аномалій (нестала кількість колонок, пусті блоки).
   - JSON-лог з таймінгами: `file`, `phase`, `duration_ms`.
