-- поиск и фиксация дубликатов с единым интерфейсом
-- общая идея: формируем ключ группы (md5 от выбранных колонок), ведем аудит, при фиксации удаляем лишние строки

-- обнаружение дубликатов
CREATE OR REPLACE FUNCTION anomaly_detect_duplicates(
    p_schema text,
    p_table text,
    p_target_columns text[] DEFAULT NULL,   -- явный список колонок ключа, если не задано, используем p_key_cols или все колонки
    p_key_cols text[] DEFAULT NULL,         -- альтернативный список ключевых колонок
    p_params jsonb DEFAULT '{}'::jsonb,     -- {"sample_limit":5}
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    cols_expr text;            -- выражение конкатенации всех колонок, если ключ не задан
    grp_expr text;             -- выражение для md5 ключа группы
    sample_limit int := COALESCE((p_params->>'sample_limit')::int, 5); -- лимит выборки ctid для аудита
    audit_id int;              -- идентификатор записи аудита
    dedup_groups bigint := 0;  -- счет групп с дубликатами
    qry text;
    rec record;
BEGIN
    -- построение выражения ключа группы
    IF array_length(p_target_columns,1) IS NOT NULL AND array_length(p_target_columns,1) > 0 THEN
        grp_expr := (
            SELECT string_agg(format('COALESCE(%I::text,%L)', col, ''), '||')
            FROM unnest(p_target_columns) AS col
        );
        grp_expr := format('md5(%s)', grp_expr);
    ELSIF array_length(p_key_cols,1) IS NOT NULL AND array_length(p_key_cols,1) > 0 THEN
        grp_expr := (
            SELECT string_agg(format('COALESCE(%I::text,%L)', col, ''), '||')
            FROM unnest(p_key_cols) AS col
        );
        grp_expr := format('md5(%s)', grp_expr);
    ELSE
        SELECT string_agg(format('COALESCE(%I::text,%L)', column_name, ''), '||' ORDER BY ordinal_position)
        INTO cols_expr
        FROM information_schema.columns
        WHERE table_schema = p_schema AND table_name = p_table;
        IF cols_expr IS NULL THEN
            RAISE EXCEPTION 'table %.% not found or has no columns', p_schema, p_table;
        END IF;
        grp_expr := format('md5(%s)', cols_expr);
    END IF;

    -- сохранение шапки аудита
    INSERT INTO dedup_audit (db_schema, db_table, key_cols, action, dry_run, details)
    VALUES (p_schema, p_table, COALESCE(p_target_columns, p_key_cols), 'duplicates_detect', p_dry_run,
            jsonb_build_object('sample_limit', sample_limit))
    RETURNING id INTO audit_id;

    -- счет групп с дубликатами и сохранение примеров
    qry := format($q$
        WITH grouped AS (
            SELECT %s AS group_md5, ctid
            FROM %I.%I
        ),
        counted AS (
            SELECT group_md5, array_agg(ctid ORDER BY ctid) AS ctids, count(*) AS cnt
            FROM grouped
            GROUP BY group_md5
            HAVING count(*) > 1
        )
        SELECT group_md5, ctids, cnt FROM counted
        ORDER BY cnt DESC
    $q$, grp_expr, p_schema, p_table);

    FOR rec IN EXECUTE qry LOOP
        dedup_groups := dedup_groups + 1;

        INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
        VALUES (
            audit_id,
            rec.group_md5::text,
            rec.ctids[1]::text,
            ARRAY(SELECT x::text FROM unnest(rec.ctids[2:LEAST(array_length(rec.ctids,1), GREATEST(2, sample_limit))]) AS x),
            'группа с дубликатами',
            jsonb_build_object('count', rec.cnt, 'ctids', ARRAY(SELECT x::text FROM unnest(rec.ctids) AS x))
        );
    END LOOP;

    -- обновление итога аудита
    UPDATE dedup_audit
    SET groups_processed = dedup_groups
    WHERE id = audit_id;

    RETURN jsonb_build_object(
        'audit_id', audit_id,
        'kind', 'duplicates',
        'mode', 'detect',
        'groups_processed', dedup_groups,
        'dry_run', p_dry_run
    );
END;
$$;

-- фиксация дубликатов
CREATE OR REPLACE FUNCTION anomaly_fix_duplicates(
    p_schema text,
    p_table text,
    p_target_columns text[] DEFAULT NULL,   -- явный ключ
    p_key_cols text[] DEFAULT NULL,         -- альтернативный ключ
    p_action text DEFAULT 'delete',         -- действие: delete
    p_params jsonb DEFAULT '{}'::jsonb,     -- {"keep":"first"|"last"}
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    grp_expr text;             -- выражение ключа
    cols_expr text;            -- запасной вариант всех колонок
    keep text := COALESCE(NULLIF(p_params->>'keep',''), 'first'); -- стратегия сохранения одной строки
    audit_id int;              -- идентификатор аудита
    dedup_groups bigint := 0;  -- счет обработанных групп
    qry text;
    rec record;
    kept_ctid text;            -- ctid оставляемой строки
    removed text[];            -- массив удаляемых ctid
    delete_q text;
BEGIN
    IF keep NOT IN ('first','last') THEN
        RAISE EXCEPTION 'invalid keep=%; allowed: first|last', keep;
    END IF;

    -- построение выражения ключа
    IF array_length(p_target_columns,1) IS NOT NULL AND array_length(p_target_columns,1) > 0 THEN
        grp_expr := (
            SELECT string_agg(format('COALESCE(%I::text,%L)', col, ''), '||')
            FROM unnest(p_target_columns) AS col
        );
        grp_expr := format('md5(%s)', grp_expr);
    ELSIF array_length(p_key_cols,1) IS NOT NULL AND array_length(p_key_cols,1) > 0 THEN
        grp_expr := (
            SELECT string_agg(format('COALESCE(%I::text,%L)', col, ''), '||')
            FROM unnest(p_key_cols) AS col
        );
        grp_expr := format('md5(%s)', grp_expr);
    ELSE
        SELECT string_agg(format('COALESCE(%I::text,%L)', column_name, ''), '||' ORDER BY ordinal_position)
        INTO cols_expr
        FROM information_schema.columns
        WHERE table_schema = p_schema AND table_name = p_table;
        IF cols_expr IS NULL THEN
            RAISE EXCEPTION 'table %.% not found or has no columns', p_schema, p_table;
        END IF;
        grp_expr := format('md5(%s)', cols_expr);
    END IF;

    -- сохранение шапки аудита
    INSERT INTO dedup_audit(db_schema, db_table, key_cols, action, dry_run, details)
    VALUES (p_schema, p_table, COALESCE(p_target_columns, p_key_cols), 'duplicates_fix', p_dry_run, jsonb_build_object('keep', keep, 'action', p_action))
    RETURNING id INTO audit_id;

    -- поиск групп и применение действия
    qry := format($q$
        SELECT group_md5, array_agg(ctid::text ORDER BY ctid) AS ctids
        FROM (
            SELECT %s AS group_md5, ctid
            FROM %I.%I
        ) t
        GROUP BY group_md5
        HAVING count(*) > 1
    $q$, grp_expr, p_schema, p_table);

    FOR rec IN EXECUTE qry LOOP
        dedup_groups := dedup_groups + 1;

        IF keep = 'last' THEN
            kept_ctid := rec.ctids[array_length(rec.ctids,1)];
        ELSE
            kept_ctid := rec.ctids[1];
        END IF;

        removed := ARRAY(SELECT x FROM unnest(rec.ctids) AS x WHERE x <> kept_ctid);

        IF p_action = 'delete' AND NOT p_dry_run AND array_length(removed,1) IS NOT NULL THEN
            delete_q := format(
                'DELETE FROM %I.%I WHERE ctid = ANY (ARRAY[%s]::tid[])',
                p_schema, p_table,
                (SELECT string_agg(quote_literal(x), ',') FROM unnest(removed) AS x)
            );
            EXECUTE delete_q;
        END IF;

        INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note)
        VALUES (
            audit_id,
            rec.group_md5,
            kept_ctid,
            removed,
            CASE WHEN p_dry_run OR p_action <> 'delete' THEN 'сухой прогон' ELSE 'удаление выполнено' END
        );
    END LOOP;

    -- обновление итога аудита
    UPDATE dedup_audit
    SET groups_processed = dedup_groups
    WHERE id = audit_id;

    RETURN jsonb_build_object(
        'audit_id', audit_id,
        'kind', 'duplicates',
        'mode', 'fix',
        'groups_processed', dedup_groups,
        'dry_run', p_dry_run
    );
END;
$$;
