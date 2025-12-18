-- таблицы метаданных для аудита операций по поиску и фиксации аномалий
-- dedup_audit хранит шапку запуска: время, схему, таблицу, ключи, действие, флаг сухого прогона, число обработанных групп и произвольные детали
CREATE TABLE dedup_audit (
    id serial PRIMARY KEY,            -- идентификатор записи аудита
    run_ts timestamptz DEFAULT now(), -- отметка времени запуска
    db_schema text,                   -- имя схемы целевой таблицы
    db_table text,                    -- имя целевой таблицы
    key_cols text[],                  -- список колонок, использованных как ключ
    action text,                      -- тип действия (detect | fix и вид аномалии)
    dry_run boolean,                  -- флаг сухого прогона
    groups_processed bigint,          -- счет обработанных групп или строк
    details jsonb                     -- произвольные детали в формате jsonb
);

-- dedup_audit_rows хранит детальные строки аудита: ключ группы, оставленный ctid, удаленные ctid, заметку и дополнительные поля
CREATE TABLE dedup_audit_rows (
    id serial PRIMARY KEY,                               -- идентификатор записи детали
    audit_id int REFERENCES dedup_audit(id) ON DELETE CASCADE, -- связь с шапкой аудита
    group_key text,                                      -- значение ключа группы или составной ключ
    kept_ctid text,                                      -- ctid оставленной строки
    removed_ctids text[],                                -- массив ctid затронутых строк
    note text,                                           -- текстовая заметка по действию
    extra jsonb                                          -- дополнительные поля в формате jsonb
);