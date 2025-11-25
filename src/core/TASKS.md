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
