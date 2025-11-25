# UI Layer Tasks

1. **Workflow Shell**
   - Реалізація навігації Import → Analyze → Review → Normalize → Materialize → Export.
   - Збереження стану між сценами.
2. **Progress & Error Panels**
   - Компонент прогрес-бару з ETA, processed rows, current action.
   - Error Log Panel із фільтрами (file, phase, severity).
3. **Import Screen**
   - Drag&Drop + file picker, відображення розміру/line count, кнопка Start Analysis.
4. **Schema Review**
   - Card view для схем, merge UI, preview normalized рядків.
   - Column normalization таблиця з кольоровими порогами.
5. **Job Runner View**
   - Live оновлення статусу матеріалізації, кнопки Show log / Stop job.
   - Історія job'ів із collapsible секціями.
6. **Virtualized Lists**
   - Компоненти для великих списків (files, blocks, columns) з lazy loading.
7. **Configuration Editor**
   - Можливість підвантажити/зберегти JSON/SQLite конфіг із Storage шару.
