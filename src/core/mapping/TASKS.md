# Mapping Module Tasks

1. **Signature Clustering**
   - Групує `FileBlock` за (`delimiter`, `column_count`, header similarity, column type profile).
   - Конфігурує пороги схожості для header (Levenshtein / n-grams).
2. **Schema Draft Builder**
   - Формує первинні `SchemaDefinition` з raw header і `ColumnStats`.
   - Відзначає колонки з low-confidence типом.
3. **Mapping Persistence**
   - Оновлює `MappingConfig.blocks[].schema_id` і серіалізує в JSON.
   - Створює індекси для швидкого пошуку всіх блоків за схемою.
4. **Merge & Split Operations**
   - API для UI: `merge(schema_a, schema_b)`, `split(blocks)`.
   - Логіка підтвердження користувачем через binary choice UI.
5. **Audit Trail**
   - Зберігає хто/коли підтвердив мапінг.
   - Логує confidence рівні для колонок.
