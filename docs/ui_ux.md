# UI / UX Notes

## Principles
- **Workflow-driven**: горизонтальний wizard Import → Analyze → Review → Normalize → Materialize → Export.
- **Progressive disclosure**: на кожному екрані тільки критично необхідні поля; розширені панелі відкриваються окремо.
- **Persistent logging**: всі помилки й попередження вбудовані в Error Log Panel, жодних модальних pop-up.
- **Virtualized lists**: довгі списки (файли, блоки, колонки, аномалії) відображаються через lazy loading.

## Screens
1. **Import Files**
   - Drag&Drop, список файлів з розміром і швидким line count.
   - Кнопка Start Analysis без додаткових рішень.
2. **Analyze Schemas**
   - Віртуалізований список файлів, статуси Running/Completed, короткі сигнатури блоків.
   - View Details → 10 raw рядків, колонкова статистика.
3. **Review & Edit Schemas**
   - Card view: name, columns, blocks count, confidence.
   - Дії: Rename, View, Delete, Merge.
4. **Column Name Normalization**
   - Таблиця Raw → Suggestion → Normalized з кольоровими порогами:
     - `>=0.90` зелений, auto-accept.
     - `0.75-0.89` блакитний, Accept/Review.
     - `0.55-0.74` жовтий, потребує підтвердження.
     - `0.40-0.54` помаранчевий, manual only.
     - `<0.40` сірий, нова колонка.
5. **Normalize Values & Types**
   - Column profiler (% нулів, % унікальних, top values, min/max).
   - Дії очищення: trim, cast, replace, date parsing.
6. **Materialization Job Runner**
   - Статус, прогрес-бар, files processed, rows processed, ETA, кнопки Show log / Stop job.
   - Історія job'ів collapsible.
   - Реал-тайм стрім `FileProgress` подій: список останніх n оновлень з ETA, rows/s та назвою файлу; click-through відкриває Storage history для конкретної схеми.
   - Панель History читає `job_metrics` + таблицю `job_progress_events` (через `storage.fetch_job_progress_events`) і показує графік rows/time + spill warning badges. Дані прунінгаться автоматично до 500 подій на схему, старі записи видаляються без впливу на job summary.
   - UI не блокує матеріалізацію: якщо job триває в іншому процесі, панель просто перепідключається до логів/телеметрії.

### Observability Surfaces
- **Live Progress Drawer**: sticky елемент із останніми `FileProgress` подіями та ручним оновленням (pull-to-refresh) для low-power режимів.
- **History Timeline**: табличний вигляд з job start/end, середня швидкість, піковий spill, лінк на parquet/db артефакти.
- **Error Log Panel**: для кожного job ID показує validation summary (short/long rows) та посилання на відповідні Job Metrics записи.
- **Retention & Cleanup**: Storage шар гарантує максимум 500 `job_progress_events` на схему; UI може викликати `storage.prune_progress_history` для ручного очищення або soft-reset історії без прямого доступу до SQL.

## Color Semantics
- Зеленій – ок.
- Жовтий – підозра.
- Червоний – помилка.
- Синій – інформація.
- Фіолетовий – AI suggestion.

## UX Guardrails
- Перед застосуванням схеми показується preview (matched files, row count, warnings).
- Merge schemas реалізується як дві карточки з binary choice `Yes, merge` / `No, keep separate`.
- Column normalization UI ніколи не залишає автоматику без підтвердження нижче 0.90 confidence.
