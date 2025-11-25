# Storage & Config Tasks

1. **Config Schema**
   - JSON структура для `MappingConfig`, версіонування через `schemaVersion`.
   - SQLite таблиці: `schemas`, `blocks`, `stats`, `synonyms`.
2. **Serialization Layer**
   - Функції `load_mapping(path)` / `save_mapping(path, data)` з валідацією.
   - Підтримка інкрементальних оновлень (append-only logs).
3. **Stats Repository**
   - Зберігає пер-колонкові метрики: `uniqueCount`, `topValues`, `min/max`.
   - API для отримання статистики на екрані Normalize Values.
4. **Job Logs**
   - Персистентний журнал job'ів: статус, files processed, errors, timestamps.
   - Індексований доступ для UI (history view).
5. **Dictionary Store**
   - API для `normalization` модуля: CRUD над словниками синонімів.
   - Підтримка merge/backup.
6. **Security & Integrity**
   - Контроль доступу до файлів (read-only vs write-enabled).
   - Checksum для великих JSON, щоб UI міг перевірити, чи потрібно перезавантажити конфіг.
