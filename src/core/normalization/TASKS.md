# Normalization Module Tasks

1. **Synonym Dictionary Engine**
   - Зберігає `NormalizedName` ↔ `KnownVariants`, підтримує імпорт/експорт JSON.
   - Вміє оновлювати словник під час Phase 1 аналізу.
2. **Fuzzy Matcher**
   - Левенштейн + n-gram scoring, видає `confidence` 0..1.
   - Використовує пороги з `docs/ui_ux.md` для категоризації кольорів.
3. **Interactive Review API**
   - Повертає таблицю Raw → Suggestion → Normalized → Confidence.
   - Дозволяє UI підтверджувати/відхиляти результати, створювати нові колонки.
4. **Value Normalizers**
   - Триммери пробілів, уніфікація `null`/`N/A`, заміна десяткових роздільників.
   - Парсери дат із fallback-стратегіями.
5. **Telemetry & Feedback Loop**
   - Збирає статистику успішних/відхилених пропозицій.
   - Передає інформацію в `storage` для наступних job'ів.
