# Schema Review Brief

## Audience
- **Design**: підготувати wireframes/cards для Review & Edit Schemas, Column Normalization, Merge dialog.
- **PM**: зафіксувати acceptance criteria та сценарії edge cases (mixed delimiters, missing headers, low confidence matches).

## Deliverables
1. **Wireframes (Figma / PNG / ASCII)**
   - Card-based grid із показниками: columns count, blocks bound, confidence + колонкові профайли (null %, unique %, top values) із `mapping.column_profiles`.
   - Merge comparison view (Schema A vs B) із CTA `Yes, merge` / `No, keep separate`.
   - Column normalization table з кольоровою легендою (зелений ≥0.90, тощо).
2. **Interaction Notes**
   - Keyboard-only навігація (focus order, shortcuts для Approve/Merge).
   - Error Log Panel states для помилок аналізу/матеріалізації.
3. **Acceptance Tests (PM)**
   - Merge scenario: два блоки з різними заголовками → manual confirmation.
   - Headerless блоки → система пропонує column_#, користувач може перейменувати.
   - Mixed delimiters → UI попереджає, дозволяє reassign schema.
   - Column profiler flags (наприклад, 0% uniqueness + 80% nulls) відображаються на карточці й блокують авто-merge до ручного підтвердження.
4. **Audit Hooks**
   - Кожне підтвердження/merge записується в SQLite `audit_log` з полями entity/action/detail/time.
5. **Version Metadata**
   - Header cluster артефакти (`mapping.header_clusters.json`) тепер несуть `artifact_version` + `version` на кожному кластері; ті ж значення кешуються у SQLite `artifact_metadata`.
   - R/O перегляди мають показувати namespace + `canonical_schema_version`, що тепер зберігається у таблиці `schemas`, щоб PM/UX бачила, до якого контракту прив'язана карта.

## Artifacts Location
- Wireframes: `docs/ui_assets/` (PNG або PDF).
- Acceptance tests: `docs/pm_acceptance.md` (таблиця Given/When/Then).
- Audit reference: `src/storage/sqlite_store.py` (`record_audit_event`).
