# Anomaly Framework (PostgreSQL)

фреймворк для поиска и фиксации аномалий в таблицах postgresql, все операции журналируются в таблицах audit

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
- kind: duplicates | missing | outliers
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
