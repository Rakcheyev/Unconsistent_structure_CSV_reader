# Data Sampling & Analysis Notes

## Binarized Sampling
- Вхід: `total_lines`, `min_gap`.
- Алгоритм `build_sample_indices` детерміновано обирає лінії:
  1. Стартові індекси `0` та `total_lines-1`.
  2. Ітеративно вставляє середини, доки проміжки > `min_gap`.
- Складність: `O(m log m)` де `m ≪ n` (кількість семплів).
- `to_block` конвертує індекс у блок `[start, end]` фіксованого розміру.

## File Analysis Flow
1. `count_lines(path)` – `O(n)`.
2. `build_sample_indices` – `O(m log m)`.
3. Потік читання файлу → буфер для потрібних блоків.
4. `build_signature(block_lines)`:
   - евристичний delimiter detection (`,`, `;`, `\t`, `|`).
   - збір `ColumnStats`: sample values, типи (`maybe_numeric`, `maybe_date`, `maybe_bool`).
   - header sample та column count (mode).
5. Формування `FileBlock` з `SchemaSignature`.

## Column Profiling
- Кожен блок додатково проходить через стрімінговий `ColumnProfiler`:
  - рахує null %, top-k значень, HLL-lite оцінку унікальних елементів;
  - відстежує min/max для чисел і дат, зберігає приклади невалідних рядків;
  - фіксує гігієну рядків (leading/trailing whitespace, printable %, sample length).
- Результати зливаються у `ColumnProfile` структуру на файлі та колонці, після чого серіалізуються в `mapping.column_profiles.json` і таблицю SQLite `column_profiles`.
- Профайли підключені до `HeaderClusterizer` (для відсіву несумісних кандидатів) і `RowNormalizer` (для фолбеку на позиції стовпчиків при конфліктах canonical names).

## Parallel Strategy
- Один процес = аналіз одного файлу (`ProcessPoolExecutor`).
- Async orchestrator управляє пулом, слухає `asyncio.as_completed` та оновлює прогрес.
- Для локального диску не використовувати `aiofiles`; звичайний buffered IO ефективніший.

## Complexity Target
`O(n) + O(m log m)` з `m ≪ n`, тобто практично `O(n)`.
Важкі кроки (кластеризація сигнатур) працюють на семплі, не на повному наборі рядків.

## Reference Implementations
- `core.analysis.line_counter.LineCounter` читає файли chunk'ами по 1 МБ у двійковому режимі, щоб швидко порахувати рядки незалежно від кодування.
- `core.analysis.block_planner.BlockPlanner` застосовує `build_sample_indices`/`to_block` правила, але стрімить блоки із буфером ≤1 МБ, щоб `AnalysisEngine` не тримав увесь блок у пам'яті.
