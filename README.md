# Anomaly Framework (PostgreSQL)

фреймворк для поиска и фиксации аномалий в таблицах postgresql, все операции журналируются в таблицах audit

## Структура проекта

### anomaly-framework.sql
Полная версия фреймворка со всеми функциями и модулями в одном файле (запустите его для подключения фреймворка к БД)

### anomaly-framework-in-parts/
Фреймворк, разделенный на отдельные логические модули для удобства разработки и изучения:
- `1 metadata.sql` - базовые таблицы аудита и вспомогательные функции
- `2 dublicates.sql` - модуль поиска и удаления дубликатов
- `3 missing.sql` - модуль обнаружения и заполнения пропусков
- `4 outliers.sql` - модуль выявления и обработки выбросов
- `5 rule-based.sql` - модуль проверки пользовательских правил
- `6 timeseries.sql` - модуль анализа временных рядов

### usage-examples/
Подробные примеры использования фреймворка:
- `upload_example_dataset.sql` - загрузка примера данных для тестирования
- `run_at_example_dataset.sql` - примеры вызовов всех функций обнаружения и исправления аномалий

## Метаданные аудита
- public.dedup_audit: шапка запуска (schema, table, key_cols, action, dry_run, groups_processed, details jsonb)
- public.dedup_audit_rows: детальные строки (group_key, kept_ctid, removed_ctids, note, extra jsonb)

## Единый интерфейс функций
```
anomaly_detect_<kind>(
    p_schema text,
    p_table text,
    p_target_columns text[] DEFAULT NULL,
    p_key_cols text[] DEFAULT NULL,
    p_params jsonb DEFAULT '{}'::jsonb,
    p_dry_run boolean DEFAULT true
) RETURNS jsonb

anomaly_fix_<kind>(
    p_schema text,
    p_table text,
    p_target_columns text[] DEFAULT NULL,
    p_key_cols text[] DEFAULT NULL,
    p_action text DEFAULT NULL,
    p_params jsonb DEFAULT '{}'::jsonb,
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
```
- kind: duplicates | missing | outliers | rule-based | timeseries
- p_schema, p_table: целевая таблица
- p_target_columns: колонки для анализа (назначение зависит от kind)
- p_key_cols: ключ для группировки/аудита; если не задан, используется ctid или все колонки (см. ниже)
- p_params: параметры метода (см. по видам)
- p_action: действие при fix (см. по видам)
- p_dry_run: true — только аудит, без изменений
- возврат: jsonb с audit_id, kind, mode, метриками и признаком dry_run

## duplicates
- detect:
  - ключ группы: p_target_columns, иначе p_key_cols, иначе все колонки таблицы
  - p_params: {"sample_limit": int, default 5} — сколько ctid сохранить в примерах
- fix:
  - p_action: 'delete' (единственное действие)
  - p_params: {"keep":"first"|"last", default "first"} — какая строка остается
  - dry_run=false удаляет лишние строки, оставляя выбранную

## missing
- detect:
  - p_target_columns: список проверяемых колонок (обязательно)
  - p_key_cols: формирование group_key в аудите; если не задано — ctid
  - p_params: {"limit_sample": int, default 100} — лимит примеров в аудите
  - текстовые колонки проверяются на null и пустую строку, остальные на null
- fix:
  - p_params.actions — карта правил по колонкам:
    - {"col":{"method":..., ...}}
    - методы: set_constant(value), set_mode, set_mean, set_median, forward_fill(order_by), backward_fill(order_by), copy_from_other_column(source_column), delete_row
  - p_action не используется (для унификации)
  - dry_run=false применяет соответствующие обновления или удаление строк

## outliers
- detect:
  - p_target_columns: ровно одна числовая колонка
  - p_params:
    - method: "iqr" | "zscore" | "mad" (default "iqr")
    - iqr: {"k": numeric, default 1.5}
    - zscore: {"threshold": numeric, default 3}
    - mad: {"threshold": numeric, default 3.5}
- fix:
  - p_action: 'flag' | 'nullify' | 'replace_with_median' | 'replace_with_mean' | 'cap' | 'delete' (default 'flag')
  - p_params: те же пороги, для flag — flag_column (default "is_outlier")
  - cap ограничивает значения в пределах порогов метода iqr/zscore/mad

## rule-based
- detect:
  - p_params.rules: массив объектов {name?, description?, severity?, expr} с SQL-условиями для поиска нарушений
  - p_key_cols/p_target_columns: используются только для примеров ключей в аудите
  - p_dry_run: всегда true (только аудит), фиксируется количество и примеры ctids/ключей
- fix:
  - p_action: глобальное действие по умолчанию 'report'; поддерживаются 'report' | 'delete' | 'set_null' | 'set_value'
  - p_params.rules: как в detect, но с полем action (если нужно переопределить p_action) и params для действий
    - set_null: params.target_columns (иначе p_target_columns)
    - set_value: params.set_value {column, value}
  - p_dry_run=true пишет только аудит; false применяет действия к строкам, выбранным по expr

## timeseries
- detect:
  - p_params.time_column: обязательный параметр с названием столбца времени (временной ряд должен быть отсортирован)
  - p_target_columns: числовые колонки для анализа; если не задано, используются все числовые столбцы
  - p_params:
    - time_column: string (обязательно) — название столбца времени
    - window_size: int, default 7 — размер скользящего окна для расчета статистик
    - z_threshold: numeric, default 3.0 — порог z-оценки для идентификации аномалий (в стандартных отклонениях от среднего)
  - p_key_cols: разделение данных на группы для независимого анализа каждой группы; если не задано, анализируется весь датасет
- fix:
  - p_action: 'replace_with_rolling_mean' (единственное действие) — замена аномальных значений на скользящее среднее
  - p_params: те же пороги window_size и z_threshold для консистентности с detect
  - dry_run=false применяет замену значений в исходной таблице для всех выявленных аномалий
