-- таблицы метаданных для аудита операций по поиску и фиксации аномалий
-- dedup_audit хранит шапку запуска: время, схему, таблицу, ключи, действие, флаг сухого прогона, число обработанных групп и произвольные детали
CREATE TABLE IF NOT EXISTS dedup_audit (
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
CREATE TABLE IF NOT EXISTS dedup_audit_rows (
    id serial PRIMARY KEY,                               -- идентификатор записи детали
    audit_id int REFERENCES dedup_audit(id) ON DELETE CASCADE, -- связь с шапкой аудита
    group_key text,                                      -- значение ключа группы или составной ключ
    kept_ctid text,                                      -- ctid оставленной строки
    removed_ctids text[],                                -- массив ctid затронутых строк
    note text,                                           -- текстовая заметка по действию
    extra jsonb                                          -- дополнительные поля в формате jsonb
);

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

-- поиск и фиксация пропусков с единым интерфейсом
-- общая идея: проверка заданных колонок на null или пустую строку (для текстовых), аудит найденных строк, гибкие стратегии заполнения

-- обнаружение пропусков
CREATE OR REPLACE FUNCTION anomaly_detect_missing(
    p_schema text,
    p_table text,
    p_target_columns text[],                 -- список колонок для проверки
    p_key_cols text[] DEFAULT NULL,          -- колонки для формирования group_key
    p_params jsonb DEFAULT '{}'::jsonb,      -- {"limit_sample":100}
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    col_name text;           -- текущая колонка
    col_info record;         -- метаданные колонки
    v_audit_id int;          -- идентификатор аудита
    v_total bigint := 0;     -- счет найденных пропусков
    v_sample_count int := 0; -- счет сохраненных примеров
    key_expr text;           -- выражение ключа для аудита
    is_text boolean;         -- флаг текстового типа
    cond text;               -- условие пропуска
    dyn_sql text;            -- динамический sql
    rec record;
    missing_cols jsonb := '[]'::jsonb; -- список колонок, которых нет в таблице
    remaining_limit int;
    limit_sample int := COALESCE((p_params->>'limit_sample')::int, 100); -- лимит примеров в аудите
BEGIN
    -- проверка существования таблицы
    IF to_regclass(p_schema || '.' || p_table) IS NULL THEN
        RAISE EXCEPTION 'table %.% does not exist', p_schema, p_table;
    END IF;

    -- обязательный список проверяемых колонок
    IF array_length(p_target_columns,1) IS NULL OR array_length(p_target_columns,1) = 0 THEN
        RAISE EXCEPTION 'p_target_columns must not be empty for missing detection';
    END IF;

    -- сохранение шапки аудита
    INSERT INTO dedup_audit (db_schema, db_table, key_cols, action, dry_run, details)
    VALUES (p_schema, p_table, p_key_cols, 'missing_detect', p_dry_run, jsonb_build_object('limit_sample', limit_sample))
    RETURNING id INTO v_audit_id;

    -- формирование ключа для аудита
    IF array_length(p_key_cols,1) IS NULL OR array_length(p_key_cols,1) = 0 THEN
        key_expr := 'ctid::text';
    ELSE
        key_expr := 'concat_ws(''|||'', ' || array_to_string(ARRAY(SELECT format('%s::text', quote_ident(k)) FROM unnest(p_key_cols) AS k), ', ') || ')';
    END IF;

    -- обход целевых колонок
    FOREACH col_name IN ARRAY p_target_columns LOOP
        SELECT column_name, data_type
        INTO col_info
        FROM information_schema.columns
        WHERE table_schema = p_schema AND table_name = p_table AND column_name = col_name;

        -- если колонки нет, фиксируем это в списке и продолжаем
        IF NOT FOUND THEN
            missing_cols := missing_cols || to_jsonb(col_name);
            CONTINUE;
        END IF;

        is_text := (col_info.data_type ILIKE '%char%' OR col_info.data_type ILIKE 'text');

        -- условие пропуска: null и пустая строка для текста, null для чисел
        IF is_text THEN
            cond := format('(%1$I IS NULL OR %1$I = '''')', col_name);
        ELSE
            cond := format('%1$I IS NULL', col_name);
        END IF;

        -- счет пропусков по колонке
        dyn_sql := format('SELECT count(*) AS cnt FROM %I.%I WHERE %s', p_schema, p_table, cond);
        EXECUTE dyn_sql INTO rec;
        v_total := v_total + COALESCE(rec.cnt,0);

        -- выборка примеров для аудита
        remaining_limit := limit_sample - v_sample_count;
        IF remaining_limit <= 0 THEN
            CONTINUE;
        END IF;

        dyn_sql := format(
            'SELECT ctid::text AS ctid, %s AS group_key, (%s)::text AS bad_val FROM %I.%I WHERE %s LIMIT %s',
            key_expr,
            quote_ident(col_name),
            p_schema,
            p_table,
            cond,
            remaining_limit
        );

        FOR rec IN EXECUTE dyn_sql LOOP
            INSERT INTO dedup_audit_rows (audit_id, group_key, kept_ctid, removed_ctids, note, extra)
            VALUES (
                v_audit_id,
                rec.group_key,
                rec.ctid,
                ARRAY[rec.ctid],
                format('пропуск в колонке %s', col_name),
                jsonb_build_object('column', col_name, 'bad_value', rec.bad_val)
            );
            v_sample_count := v_sample_count + 1;
            EXIT WHEN v_sample_count >= limit_sample;
        END LOOP;

        EXIT WHEN v_sample_count >= limit_sample;
    END LOOP;

    -- итог аудита
    UPDATE dedup_audit
    SET groups_processed = v_total,
        details = coalesce(details, '{}'::jsonb) || jsonb_build_object(
            'detected_missing_rows', v_total,
            'sample_saved', v_sample_count,
            'missing_columns', missing_cols
        )
    WHERE id = v_audit_id;

    RETURN jsonb_build_object(
        'audit_id', v_audit_id,
        'kind', 'missing',
        'mode', 'detect',
        'detected_rows', v_total,
        'sample_saved', v_sample_count,
        'missing_columns', missing_cols,
        'dry_run', p_dry_run
    );
END;
$$;

-- фиксация пропусков
CREATE OR REPLACE FUNCTION anomaly_fix_missing(
    p_schema text,
    p_table text,
    p_target_columns text[] DEFAULT NULL,    -- для унификации
    p_key_cols text[] DEFAULT NULL,          -- для унификации
    p_action text DEFAULT NULL,              -- для унификации, логика в p_params.actions
    p_params jsonb DEFAULT '{}'::jsonb,      -- { "actions": { "col": { "method": "...", ... } } }
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    v_audit_id int;          -- идентификатор аудита
    v_total bigint := 0;     -- счет измененных строк

    rec record;              -- цикл по jsonb

    v_col text;              -- имя колонки
    v_method text;           -- метод обработки

    v_col_type text;         -- тип колонки
    v_is_missing text;       -- условие пропуска
    v_is_present text;       -- условие непустого значения

    v_sql text;              -- sql для исполнения
    v_fill_value text;       -- значение для заполнения

    v_source_col text;       -- источник для copy_from_other_column
    v_order_by text;         -- сортировка для forward/backward fill
    v_rowcount bigint;       -- счет затронутых строк
BEGIN
    -- сохранение шапки аудита
    INSERT INTO dedup_audit (
        db_schema, db_table, key_cols, action, dry_run, details
    )
    VALUES (
        p_schema, p_table, p_key_cols,
        'missing_fix',
        p_dry_run,
        jsonb_build_object('actions', p_params->'actions')
    )
    RETURNING id INTO v_audit_id;

    IF p_params->'actions' IS NULL THEN
        RAISE EXCEPTION 'missing fix requires p_params.actions map';
    END IF;

    -- обход правил по колонкам
    FOR rec IN
        SELECT key, value
        FROM jsonb_each(p_params->'actions')
    LOOP
        v_col    := rec.key;
        v_method := rec.value->>'method';

        SELECT format_type(a.atttypid, a.atttypmod)
        INTO v_col_type
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema
          AND c.relname = p_table
          AND a.attname = v_col
          AND a.attnum > 0
          AND NOT a.attisdropped;

        IF v_col_type IS NULL THEN
            RAISE EXCEPTION 'column %.% does not exist', p_table, v_col;
        END IF;

        -- условия пропуска и присутствия
        IF v_col_type ILIKE '%char%' OR v_col_type ILIKE '%text%' THEN
            v_is_missing := format('%I IS NULL OR %I = ''''', v_col, v_col);
            v_is_present := format('%I IS NOT NULL AND %I <> ''''', v_col, v_col);
        ELSE
            v_is_missing := format('%I IS NULL', v_col);
            v_is_present := format('%I IS NOT NULL', v_col);
        END IF;

        -- выбор метода
        IF v_method = 'set_constant' THEN
            v_sql := format(
                'UPDATE %I.%I SET %I = %s WHERE %s',
                p_schema, p_table,
                v_col,
                quote_literal(rec.value->>'value') || '::' || v_col_type,
                v_is_missing
            );

        ELSIF v_method = 'set_mode' THEN
            EXECUTE format(
                'SELECT %I FROM %I.%I
                 WHERE %s
                 GROUP BY %I
                 ORDER BY count(*) DESC
                 LIMIT 1',
                v_col, p_schema, p_table,
                v_is_present,
                v_col
            ) INTO v_fill_value;

            IF v_fill_value IS NULL THEN
                CONTINUE;
            END IF;

            v_sql := format(
                'UPDATE %I.%I SET %I = %s WHERE %s',
                p_schema, p_table,
                v_col,
                quote_literal(v_fill_value) || '::' || v_col_type,
                v_is_missing
            );

        ELSIF v_method = 'set_mean' THEN
            EXECUTE format(
                'SELECT avg(%I)::%s FROM %I.%I WHERE %I IS NOT NULL',
                v_col, v_col_type,
                p_schema, p_table,
                v_col
            ) INTO v_fill_value;

            v_sql := format(
                'UPDATE %I.%I SET %I = %s WHERE %s',
                p_schema, p_table,
                v_col,
                v_fill_value,
                v_is_missing
            );

        ELSIF v_method = 'set_median' THEN
            EXECUTE format(
                'SELECT percentile_cont(0.5)
                 WITHIN GROUP (ORDER BY %I)::%s
                 FROM %I.%I
                 WHERE %I IS NOT NULL',
                v_col, v_col_type,
                p_schema, p_table,
                v_col
            ) INTO v_fill_value;

            v_sql := format(
                'UPDATE %I.%I SET %I = %s WHERE %s',
                p_schema, p_table,
                v_col,
                v_fill_value,
                v_is_missing
            );

        ELSIF v_method = 'forward_fill' THEN
            v_order_by := rec.value->>'order_by';
            IF v_order_by IS NULL THEN
                RAISE EXCEPTION 'forward_fill requires order_by';
            END IF;

            v_sql := format($f$
                WITH numbered AS (
                    SELECT ctid, %I, row_number() OVER (ORDER BY %I) AS rn
                    FROM %I.%I
                ), filled AS (
                    SELECT ctid,
                           coalesce(%I,
                             lag(%I) OVER (ORDER BY rn)
                           ) AS val
                    FROM numbered
                )
                UPDATE %I.%I t
                SET %I = f.val
                FROM filled f
                WHERE t.ctid = f.ctid AND %s
            $f$,
                v_col, v_order_by,
                p_schema, p_table,
                v_col, v_col,
                p_schema, p_table,
                v_col,
                v_is_missing
            );

        ELSIF v_method = 'backward_fill' THEN
            v_order_by := rec.value->>'order_by';
            IF v_order_by IS NULL THEN
                RAISE EXCEPTION 'backward_fill requires order_by';
            END IF;

            v_sql := format($b$
                WITH numbered AS (
                    SELECT ctid, %I, row_number() OVER (ORDER BY %I DESC) AS rn
                    FROM %I.%I
                ), filled AS (
                    SELECT ctid,
                           coalesce(%I,
                             lag(%I) OVER (ORDER BY rn)
                           ) AS val
                    FROM numbered
                )
                UPDATE %I.%I t
                SET %I = f.val
                FROM filled f
                WHERE t.ctid = f.ctid AND %s
            $b$,
                v_col, v_order_by,
                p_schema, p_table,
                v_col, v_col,
                p_schema, p_table,
                v_col,
                v_is_missing
            );

        ELSIF v_method = 'copy_from_other_column' THEN
            v_source_col := rec.value->>'source_column';
            IF v_source_col IS NULL THEN
                RAISE EXCEPTION 'copy_from_other_column requires source_column';
            END IF;

            v_sql := format(
                'UPDATE %I.%I SET %I = %I WHERE %s AND %I IS NOT NULL',
                p_schema, p_table,
                v_col, v_source_col,
                v_is_missing,
                v_source_col
            );

        ELSIF v_method = 'delete_row' THEN
            v_sql := format(
                'DELETE FROM %I.%I WHERE %s',
                p_schema, p_table,
                v_is_missing
            );

        ELSE
            RAISE EXCEPTION 'unknown method: %', v_method;
        END IF;

        -- применение или сухой прогон
        IF NOT p_dry_run THEN
            EXECUTE v_sql;
            GET DIAGNOSTICS v_rowcount = ROW_COUNT;
            v_total := v_total + v_rowcount;
        END IF;
    END LOOP;

    -- итог аудита
    UPDATE dedup_audit
    SET groups_processed = v_total
    WHERE id = v_audit_id;

    RETURN jsonb_build_object(
        'audit_id', v_audit_id,
        'kind', 'missing',
        'mode', 'fix',
        'rows_affected', v_total,
        'dry_run', p_dry_run
    );
END;
$$;

-- поиск и фиксация выбросов с единым интерфейсом
-- общая идея: статистика по числовой колонке, вычисление порогов по выбранному методу, аудит найденных строк, опциональное исправление

-- внутренний модуль для повторного использования
CREATE OR REPLACE FUNCTION _outlier_core(
    p_schema text,
    p_table text,
    p_col text,
    p_key_cols text[] DEFAULT NULL,
    p_action text DEFAULT NULL,
    p_params jsonb DEFAULT '{}'::jsonb,
    p_dry_run boolean DEFAULT true,
    p_mode text DEFAULT 'detect'             -- detect | fix
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    rel regclass;              -- ссылка на таблицу
    col_type text;             -- тип целевой колонки
    n_count bigint;            -- счет непустых значений
    mean_val double precision; -- среднее
    stddev_val double precision; -- стандартное отклонение
    median_val double precision; -- медиана
    q1_val double precision;   -- нижний квартиль
    q3_val double precision;   -- верхний квартиль
    iqr_val double precision;  -- межквартильный размах
    lower_bound double precision; -- нижняя граница
    upper_bound double precision; -- верхняя граница
    mad_val double precision;  -- медианное абсолютное отклонение
    method text := COALESCE(NULLIF(p_params->>'method',''), 'iqr'); -- выбранный метод
    k numeric := COALESCE((p_params->>'k')::numeric, 1.5);          -- множитель для iqr
    z_threshold numeric := COALESCE((p_params->>'threshold')::numeric, 3);   -- порог zscore
    mad_threshold numeric := COALESCE((p_params->>'threshold')::numeric, 3.5); -- порог mad
    flag_column text := COALESCE(NULLIF(p_params->>'flag_column',''), 'is_outlier'); -- имя колонки флага
    audit_id int;              -- идентификатор аудита
    out_count bigint := 0;     -- счет выбросов
    cond_sql text;             -- условие для выбросов
    key_expr text;             -- ключ для аудита
    rec record;
    detail jsonb;              -- подробности для details
BEGIN
    -- проверка таблицы
    BEGIN
        rel := format('%I.%I', p_schema, p_table)::regclass;
    EXCEPTION WHEN others THEN
        RAISE EXCEPTION 'table %.% does not exist or not accessible', p_schema, p_table;
    END;

    -- проверка колонки и ее типа
    SELECT format_type(a.atttypid, a.atttypmod)
    INTO col_type
    FROM pg_attribute a
    WHERE a.attrelid = rel AND a.attname = p_col AND NOT a.attisdropped;

    IF col_type IS NULL THEN
        RAISE EXCEPTION 'column % not found in %.%', p_col, p_schema, p_table;
    END IF;

    IF col_type NOT ILIKE 'smallint%' AND
       col_type NOT ILIKE 'integer%'  AND
       col_type NOT ILIKE 'bigint%'   AND
       col_type NOT ILIKE 'real%'     AND
       col_type NOT ILIKE 'double precision%' AND
       col_type NOT ILIKE 'numeric%' AND
       col_type NOT ILIKE 'decimal%' THEN
        RAISE EXCEPTION 'column % in %.% must be numeric type, found %', p_col, p_schema, p_table, col_type;
    END IF;

    -- сбор статистики по колонке
    EXECUTE format(
        'SELECT count(%1$s::double precision), avg(%1$s::double precision), stddev_pop(%1$s::double precision), ' ||
        'percentile_cont(0.5) WITHIN GROUP (ORDER BY %1$s::double precision), ' ||
        'percentile_cont(0.25) WITHIN GROUP (ORDER BY %1$s::double precision), ' ||
        'percentile_cont(0.75) WITHIN GROUP (ORDER BY %1$s::double precision) ' ||
        'FROM %I.%I WHERE %1$s IS NOT NULL',
        quote_ident(p_col), p_schema, p_table
    )
    INTO n_count, mean_val, stddev_val, median_val, q1_val, q3_val;

    iqr_val := COALESCE(q3_val,0) - COALESCE(q1_val,0);

    -- выбор метода и формирование условия выбросов
    IF method = 'iqr' THEN
        lower_bound := q1_val - k * iqr_val;
        upper_bound := q3_val + k * iqr_val;
        cond_sql := format('%I::double precision < %L OR %I::double precision > %L', p_col, lower_bound, p_col, upper_bound);

    ELSIF method = 'zscore' THEN
        IF stddev_val IS NULL OR stddev_val = 0 THEN
            RAISE EXCEPTION 'standard deviation is zero or null; zscore method not applicable';
        END IF;
        cond_sql := format('abs((%I::double precision - %L) / %L) > %L', p_col, mean_val, stddev_val, z_threshold);

    ELSIF method = 'mad' THEN
        EXECUTE format(
            'SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(%I::double precision - %L::double precision)) FROM %I.%I WHERE %I IS NOT NULL',
            p_col, median_val, p_schema, p_table, p_col
        ) INTO mad_val;

        IF mad_val IS NULL OR mad_val = 0 THEN
            out_count := 0;
            detail := jsonb_build_object(
                'method', method,
                'params', p_params,
                'n', n_count,
                'outliers', out_count,
                'mean', mean_val,
                'stddev', stddev_val,
                'median', median_val,
                'q1', q1_val,
                'q3', q3_val,
                'iqr', iqr_val,
                'mad', mad_val,
                'note', 'mad равно нулю или null, выбросы не выявлены'
            );

            INSERT INTO dedup_audit(db_schema, db_table, key_cols, action, dry_run, groups_processed, details)
            VALUES (p_schema, p_table, p_key_cols, CASE WHEN p_mode='fix' THEN 'outliers_fix' ELSE 'outliers_detect' END, p_dry_run, out_count, detail)
            RETURNING id INTO audit_id;

            RETURN jsonb_build_object(
                'audit_id', audit_id,
                'kind', 'outliers',
                'mode', p_mode,
                'found_outliers', out_count,
                'dry_run', p_dry_run,
                'details', detail
            );
        END IF;

        cond_sql := format('abs(0.6745 * (%I::double precision - %L) / %L) > %L', p_col, median_val, mad_val, mad_threshold);

    ELSE
        RAISE EXCEPTION 'unknown method: %', method;
    END IF;

    -- счет выбросов
    EXECUTE format('SELECT count(*) FROM %I.%I WHERE %s', p_schema, p_table, cond_sql) INTO out_count;

    -- детализация для аудита
    detail := jsonb_build_object(
        'method', method,
        'params', p_params,
        'n', n_count,
        'outliers', out_count,
        'mean', mean_val,
        'stddev', stddev_val,
        'median', median_val,
        'q1', q1_val,
        'q3', q3_val,
        'iqr', iqr_val,
        'mad', mad_val,
        'lower', lower_bound,
        'upper', upper_bound
    );

    -- сохранение шапки аудита
    INSERT INTO dedup_audit(db_schema, db_table, key_cols, action, dry_run, groups_processed, details)
    VALUES (p_schema, p_table, p_key_cols, CASE WHEN p_mode='fix' THEN 'outliers_fix' ELSE 'outliers_detect' END, p_dry_run, out_count, detail)
    RETURNING id INTO audit_id;

    -- формирование ключа для аудита строк
    IF p_key_cols IS NULL OR array_length(p_key_cols,1) IS NULL THEN
        key_expr := 'ctid::text';
    ELSE
        key_expr := array_to_string(ARRAY(
            SELECT format('COALESCE(%I::text, '''')', col) FROM unnest(p_key_cols) AS col
        ), ' || ''|'' || ');
    END IF;

    -- сохранение строк с выбросами в аудит
    FOR rec IN EXECUTE format(
        'SELECT ctid::text AS ctid_txt, %s AS group_key, %I::double precision AS val FROM %I.%I WHERE %s',
        key_expr, p_col, p_schema, p_table, cond_sql
    ) LOOP
        INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
        VALUES (
            audit_id,
            rec.group_key,
            rec.ctid_txt,
            NULL,
            'выброс обнаружен',
            jsonb_build_object('column', p_col, 'value', rec.val)
        );
    END LOOP;

    -- применение исправления при необходимости
    IF p_mode = 'fix' AND NOT p_dry_run THEN
        IF p_action = 'flag' THEN
            EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS %I boolean DEFAULT false', p_schema, p_table, flag_column);
            EXECUTE format('UPDATE %I.%I SET %I = true WHERE %s', p_schema, p_table, flag_column, cond_sql);

        ELSIF p_action = 'nullify' THEN
            EXECUTE format('UPDATE %I.%I SET %I = NULL WHERE %s', p_schema, p_table, p_col, cond_sql);

        ELSIF p_action = 'replace_with_median' THEN
            EXECUTE format('UPDATE %I.%I SET %I = (%L)::%s WHERE %s', p_schema, p_table, p_col, median_val, col_type, cond_sql);

        ELSIF p_action = 'replace_with_mean' THEN
            EXECUTE format('UPDATE %I.%I SET %I = (%L)::%s WHERE %s', p_schema, p_table, p_col, mean_val, col_type, cond_sql);

        ELSIF p_action = 'cap' THEN
            EXECUTE format(
                'UPDATE %I.%I SET %I = (LEAST(GREATEST(%I::double precision, %L::double precision), %L::double precision))::%s WHERE %s',
                p_schema, p_table, p_col, p_col, lower_bound, upper_bound, col_type, cond_sql
            );

        ELSIF p_action = 'delete' THEN
            EXECUTE format('DELETE FROM %I.%I WHERE %s', p_schema, p_table, cond_sql);

        ELSE
            NULL;
        END IF;
    END IF;

    RETURN jsonb_build_object(
        'audit_id', audit_id,
        'kind', 'outliers',
        'mode', p_mode,
        'found_outliers', out_count,
        'dry_run', p_dry_run,
        'details', detail
    );
END;
$$;

-- обнаружение выбросов
CREATE OR REPLACE FUNCTION anomaly_detect_outliers(
    p_schema text,
    p_table text,
    p_target_columns text[],                 -- ровно одна числовая колонка
    p_key_cols text[] DEFAULT NULL,
    p_params jsonb DEFAULT '{}'::jsonb,
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    col text;
BEGIN
    IF array_length(p_target_columns,1) <> 1 THEN
        RAISE EXCEPTION 'outliers detect requires exactly one target column';
    END IF;
    col := p_target_columns[1];
    RETURN _outlier_core(p_schema, p_table, col, p_key_cols, NULL, p_params, p_dry_run, 'detect');
END;
$$;

-- фиксация выбросов
CREATE OR REPLACE FUNCTION anomaly_fix_outliers(
    p_schema text,
    p_table text,
    p_target_columns text[],                 -- ровно одна числовая колонка
    p_key_cols text[] DEFAULT NULL,
    p_action text DEFAULT 'flag',            -- flag | nullify | replace_with_median | replace_with_mean | cap | delete
    p_params jsonb DEFAULT '{}'::jsonb,
    p_dry_run boolean DEFAULT true
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    col text;
BEGIN
    IF array_length(p_target_columns,1) <> 1 THEN
        RAISE EXCEPTION 'outliers fix requires exactly one target column';
    END IF;
    col := p_target_columns[1];
    RETURN _outlier_core(p_schema, p_table, col, p_key_cols, p_action, p_params, p_dry_run, 'fix');
END;
$$;

-- поиск и фиксация аномалий на основе пользовательских правил (rule-based)
-- общая идея: передача списка правил с sql-выражениями, проверка строк на соответствие, аудит нарушений и применение действий

-- обнаружение аномалий rule-based
CREATE OR REPLACE FUNCTION anomaly_detect_rule_based(
    p_schema TEXT,
    p_table TEXT,
    p_target_columns TEXT[] DEFAULT NULL,
    p_key_cols TEXT[] DEFAULT NULL,
    p_params JSONB DEFAULT '{}'::JSONB,
    p_dry_run BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql AS
$$
DECLARE
    v_exists BOOLEAN;
    v_rule JSONB;
    v_rules JSONB := COALESCE(p_params->'rules', '[]'::JSONB);
    v_audit_id INT;
    v_sql TEXT;
    v_count BIGINT;
    v_ctids TEXT[];
    v_sample_keys JSONB;
    v_key_list TEXT := '';
    v_target_list TEXT := '';
    v_idx INT := 0;
    v_rules_summary JSONB := '[]'::JSONB;
BEGIN
    -- проверка существования таблицы
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = p_schema AND table_name = p_table
    ) INTO v_exists;

    IF NOT v_exists THEN
        RETURN JSONB_BUILD_OBJECT(
            'status','error',
            'message', FORMAT('таблица %s.%s не найдена', p_schema, p_table)
        );
    END IF;

    -- подготовка списка ключевых и целевых колонок
    IF p_key_cols IS NOT NULL AND array_length(p_key_cols,1) > 0 THEN
        v_key_list := array_to_string((
            SELECT array_agg(FORMAT('%s', quote_ident(col))) FROM unnest(p_key_cols) col
        ), ', ');
    END IF;

    IF p_target_columns IS NOT NULL AND array_length(p_target_columns,1) > 0 THEN
        v_target_list := array_to_string((
            SELECT array_agg(FORMAT('%s', quote_ident(col))) FROM unnest(p_target_columns) col
        ), ', ');
    END IF;

    -- создание записи аудита
    INSERT INTO dedup_audit(db_schema, db_table, key_cols, action, dry_run, groups_processed, details)
    VALUES (p_schema, p_table, p_key_cols, 'detect_rule_based', p_dry_run, 0, '{}'::JSONB)
    RETURNING id INTO v_audit_id;

    -- итерация по правилам
    FOR v_rule IN SELECT * FROM jsonb_array_elements(v_rules)
    LOOP
        v_idx := v_idx + 1;
        IF (v_rule->>'expr') IS NULL THEN
            v_rules_summary := v_rules_summary || JSONB_BUILD_OBJECT(
                'name', COALESCE(v_rule->>'name', FORMAT('rule_%s', v_idx)),
                'status', 'skipped',
                'reason', 'отсутствует выражение expr'
            );
            CONTINUE;
        END IF;

        -- подсчет нарушений правила
        v_sql := FORMAT('SELECT COUNT(*)::BIGINT FROM %I.%I WHERE %s',
                        p_schema, p_table, v_rule->>'expr');
        EXECUTE v_sql INTO v_count;

        -- выборка ctid строк с нарушениями
        v_sql := FORMAT('SELECT array_agg(ctid::TEXT) FROM (SELECT ctid FROM %I.%I WHERE %s LIMIT 100) t',
                        p_schema, p_table, v_rule->>'expr');
        EXECUTE v_sql INTO v_ctids;

        -- выборка ключевых значений для примеров
        IF v_key_list <> '' THEN
            v_sql := FORMAT(
                'SELECT COALESCE(JSONB_AGG(TO_JSONB(t)), ''[]''::JSONB) FROM (SELECT %s FROM %I.%I WHERE %s LIMIT 100) t',
                v_key_list, p_schema, p_table, v_rule->>'expr'
            );
            EXECUTE v_sql INTO v_sample_keys;
        ELSIF v_target_list <> '' THEN
            v_sql := FORMAT(
                'SELECT COALESCE(JSONB_AGG(TO_JSONB(t)), ''[]''::JSONB) FROM (SELECT %s FROM %I.%I WHERE %s LIMIT 100) t',
                v_target_list, p_schema, p_table, v_rule->>'expr'
            );
            EXECUTE v_sql INTO v_sample_keys;
        ELSE
            v_sample_keys := '[]'::JSONB;
        END IF;

        -- запись строки аудита для правила
        INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
        VALUES (
            v_audit_id,
            COALESCE(v_rule->>'name', FORMAT('rule_%s', v_idx)),
            NULL,
            v_ctids,
            COALESCE(v_rule->>'description', NULL),
            JSONB_BUILD_OBJECT(
                'expr', v_rule->>'expr',
                'severity', v_rule->>'severity',
                'count', v_count,
                'sample_keys', v_sample_keys
            )
        );

        -- добавление в сводку
        v_rules_summary := v_rules_summary || JSONB_BUILD_OBJECT(
            'name', COALESCE(v_rule->>'name', FORMAT('rule_%s', v_idx)),
            'expr', v_rule->>'expr',
            'severity', v_rule->>'severity',
            'count', v_count,
            'sample_keys', v_sample_keys,
            'sample_ctids', v_ctids
        );
    END LOOP;

    -- обновление общей записи аудита
    UPDATE dedup_audit
    SET groups_processed = (SELECT COUNT(*) FROM dedup_audit_rows WHERE audit_id = v_audit_id),
        details = JSONB_BUILD_OBJECT(
            'rules_count', JSONB_ARRAY_LENGTH(v_rules),
            'rules', v_rules_summary
        )
    WHERE id = v_audit_id;

    RETURN JSONB_BUILD_OBJECT(
        'status','ok',
        'audit_id', v_audit_id,
        'details', (SELECT details FROM dedup_audit WHERE id = v_audit_id)
    );
END;
$$;

-- исправление аномалий rule-based
CREATE OR REPLACE FUNCTION anomaly_fix_rule_based(
    p_schema TEXT,
    p_table TEXT,
    p_target_columns TEXT[] DEFAULT NULL,
    p_key_cols TEXT[] DEFAULT NULL,
    p_action TEXT DEFAULT NULL,
    p_params JSONB DEFAULT '{}'::JSONB,
    p_dry_run BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql AS
$$
DECLARE
    v_exists BOOLEAN;
    v_rule JSONB;
    v_rules JSONB := COALESCE(p_params->'rules', '[]'::JSONB);
    v_audit_id INT;
    v_idx INT := 0;
    v_ctids tid[];
    v_count BIGINT;
    v_stmt_count BIGINT := 0;
    v_groups_processed BIGINT := 0;
    v_action_rule TEXT;
    v_rule_params JSONB;
    v_set_col TEXT;
    v_set_val TEXT;
    v_set_list TEXT[];
    v_sql TEXT;
BEGIN
    -- проверка существования таблицы
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = p_schema AND table_name = p_table
    ) INTO v_exists;

    IF NOT v_exists THEN
        RETURN JSONB_BUILD_OBJECT(
            'status','error',
            'message', FORMAT('таблица %s.%s не найдена', p_schema, p_table)
        );
    END IF;

    -- создание записи аудита
    INSERT INTO dedup_audit(db_schema, db_table, key_cols, action, dry_run, groups_processed, details)
    VALUES (p_schema, p_table, p_key_cols, 'fix_rule_based', p_dry_run, 0, '{}'::JSONB)
    RETURNING id INTO v_audit_id;

    -- обработка правил
    FOR v_rule IN SELECT * FROM jsonb_array_elements(v_rules)
    LOOP
        v_idx := v_idx + 1;

        IF (v_rule->>'expr') IS NULL THEN
            INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
            VALUES (
                v_audit_id,
                COALESCE(v_rule->>'name', FORMAT('rule_%s', v_idx)),
                NULL,
                NULL,
                'пропущено: отсутствует expr',
                JSONB_BUILD_OBJECT('raw_rule', v_rule)
            );
            CONTINUE;
        END IF;

        -- определение действия для правила
        v_action_rule := COALESCE(p_action, v_rule->>'action', (p_params->>'action'), 'report');
        v_rule_params := COALESCE(v_rule->'params', '{}'::JSONB);

        -- сбор массива ctids строк, нарушающих правило
        v_sql := FORMAT('SELECT ARRAY(SELECT ctid FROM %I.%I WHERE %s)', p_schema, p_table, v_rule->>'expr');
        EXECUTE v_sql INTO v_ctids;

        v_count := COALESCE(array_length(v_ctids,1),0);

        -- dry-run или report: только запись в аудит
        IF v_action_rule = 'report' OR p_dry_run THEN
            INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
            VALUES (
                v_audit_id,
                COALESCE(v_rule->>'name', FORMAT('rule_%s', v_idx)),
                NULL,
                ARRAY(SELECT ctid::TEXT FROM unnest(v_ctids) AS ctid),
                COALESCE(v_rule->>'description', NULL),
                JSONB_BUILD_OBJECT('expr', v_rule->>'expr', 'planned_action', v_action_rule, 'count', v_count)
            );

        ELSE
            -- действие delete: удаление строк
            IF v_action_rule = 'delete' AND v_count > 0 THEN
                v_sql := FORMAT('DELETE FROM %I.%I WHERE ctid = ANY($1::tid[])', p_schema, p_table);
                EXECUTE v_sql USING v_ctids;
                v_stmt_count := v_stmt_count + v_count;

            -- действие set_null: установка null в целевых колонках
            ELSIF v_action_rule = 'set_null' AND v_count > 0 THEN
                IF (v_rule_params->'target_columns') IS NOT NULL THEN
                    SELECT array_agg(quote_ident(value::TEXT)) 
                    INTO v_set_list
                    FROM jsonb_array_elements_text(v_rule_params->'target_columns');
                ELSIF p_target_columns IS NOT NULL THEN
                    SELECT array_agg(quote_ident(col))
                    INTO v_set_list
                    FROM unnest(p_target_columns) AS col;
                END IF;

                IF v_set_list IS NOT NULL THEN
                    v_sql := format('UPDATE %I.%I SET %s = NULL WHERE ctid = ANY($1::tid[])',
                                    p_schema, p_table, array_to_string(v_set_list, ' = NULL, ') || ' = NULL');
                    EXECUTE v_sql USING v_ctids;
                    v_stmt_count := v_stmt_count + v_count;
                END IF;

            -- действие set_value: установка конкретного значения в указанной колонке
            ELSIF v_action_rule = 'set_value' AND v_count > 0 THEN
                v_set_col := v_rule_params->'set_value'->>'column';
                v_set_val := v_rule_params->'set_value'->>'value';
                IF v_set_col IS NOT NULL THEN
                    v_sql := FORMAT('UPDATE %I.%I SET %s = %s WHERE ctid = ANY($1::tid[])',
                                    p_schema, p_table, quote_ident(v_set_col), quote_nullable(v_set_val));
                    EXECUTE v_sql USING v_ctids;
                    v_stmt_count := v_stmt_count + v_count;
                END IF;
            END IF;

            -- запись результатов в аудит
            INSERT INTO dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
            VALUES (
                v_audit_id,
                COALESCE(v_rule->>'name', FORMAT('rule_%s', v_idx)),
                NULL,
                ARRAY(SELECT ctid::TEXT FROM unnest(v_ctids) AS ctid),
                COALESCE(v_rule->>'description', NULL),
                JSONB_BUILD_OBJECT('expr', v_rule->>'expr', 'action', v_action_rule, 'affected', v_count)
            );
        END IF;

        v_groups_processed := v_groups_processed + 1;
    END LOOP;

    -- обновление общей записи аудита
    UPDATE dedup_audit
    SET groups_processed = v_groups_processed,
        details = JSONB_BUILD_OBJECT(
            'rules_count', JSONB_ARRAY_LENGTH(v_rules),
            'executed_rows', v_stmt_count,
            'dry_run', p_dry_run
        )
    WHERE id = v_audit_id;

    RETURN JSONB_BUILD_OBJECT(
        'status','ok',
        'audit_id', v_audit_id,
        'groups_processed', v_groups_processed,
        'executed_rows', v_stmt_count,
        'dry_run', p_dry_run
    );

EXCEPTION
    WHEN OTHERS THEN
        UPDATE dedup_audit
        SET details = JSONB_BUILD_OBJECT('error', SQLSTATE, 'message', SQLERRM)
        WHERE id = v_audit_id;
        RAISE;
END;
$$;

-- обнаружение аномалий во временных рядах с использованием скользящих статистик
CREATE OR REPLACE FUNCTION anomaly_detect_timeseries(
    p_schema TEXT,
    p_table TEXT,
    p_target_columns TEXT[] DEFAULT NULL,
    p_key_cols TEXT[] DEFAULT NULL,
    p_params JSONB DEFAULT '{}'::JSONB,
    p_dry_run BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_time_col TEXT;
    v_window_size INT := COALESCE((p_params->>'window_size')::INT, 7);
    v_z_threshold NUMERIC := COALESCE((p_params->>'z_threshold')::NUMERIC, 3.0);
    v_target_cols TEXT[];
    v_group_expr TEXT;
    v_partition_cols TEXT;
    v_tmp_table TEXT;
    v_sql TEXT;
    v_audit_id INT;
    v_groups BIGINT := 0;
    v_anoms BIGINT := 0;
    rec TEXT;
BEGIN
    -- выделение имени столбца времени из параметров для использования в оконных функциях
    v_time_col := p_params->>'time_column';
    IF v_time_col IS NULL THEN
        RAISE EXCEPTION 'p_params must contain time_column';
    END IF;

    -- автоматическое определение целевых числовых столбцов из информационной схемы базы данных
    -- при отсутствии явно переданного списка анализируются все колонки с числовыми типами
    IF p_target_columns IS NULL THEN
        SELECT array_agg(column_name::TEXT)
        INTO v_target_cols
        FROM information_schema.columns
        WHERE table_schema = p_schema
          AND table_name = p_table
          AND data_type IN (
              'smallint','integer','bigint',
              'numeric','real','double precision','decimal'
          )
          AND column_name <> v_time_col
          AND (p_key_cols IS NULL OR column_name <> ALL(p_key_cols));
    ELSE
        v_target_cols := p_target_columns;
    END IF;

    IF v_target_cols IS NULL OR array_length(v_target_cols,1) = 0 THEN
        RAISE EXCEPTION 'не найдено числовых колонок для анализа';
    END IF;

    -- логирование инициирования анализа в аудит-таблице с сохранением параметров вызова
    -- запись включает временную метку, схему, таблицу и указанные ключевые колонки
    INSERT INTO dedup_audit(
        run_ts, db_schema, db_table, key_cols,
        action, dry_run, groups_processed, details
    )
    VALUES (
        NOW(), p_schema, p_table, p_key_cols,
        'detect_timeseries', p_dry_run, 0, '{}'::JSONB
    )
    RETURNING id INTO v_audit_id;

    v_tmp_table := FORMAT('__anomaly_ts_%s', v_audit_id);

    -- инициализация временной таблицы для накопления результатов анализа
    -- таблица хранит ключевые данные, значения временного ряда, скользящее среднее, стандартное отклонение и z-оценки
    EXECUTE FORMAT(
        'CREATE TEMP TABLE %I (
            group_key TEXT,
            ts TIMESTAMPTZ,
            target_col TEXT,
            value NUMERIC,
            rolling_mean NUMERIC,
            rolling_std NUMERIC,
            z_score NUMERIC
        ) ON COMMIT DROP',
        v_tmp_table
    );

    -- формирование выражений для логического разделения данных по ключевым столбцам
    -- при отсутствии ключей используется глобальная группа для обработки всего датасета единой аналитикой
    IF p_key_cols IS NULL OR array_length(p_key_cols,1) = 0 THEN
        v_group_expr := '''__ALL__''::text';
        v_partition_cols := NULL;
    ELSE
        SELECT string_agg(FORMAT('t.%I', k), ', ')
        INTO v_partition_cols
        FROM unnest(p_key_cols) AS k;

        SELECT string_agg(
            FORMAT('COALESCE(t.%I::text, ''#'')', k),
            ' || ''|'' || '
        )
        INTO v_group_expr
        FROM unnest(p_key_cols) AS k;

        v_group_expr := FORMAT('(%s)::text', v_group_expr);
    END IF;

    -- вычисление скользящих статистик для каждого столбца датасета
    -- расчет выполняется в окно из последних n значений (где n = window_size) и включает среднее значение, стандартное отклонение
    -- нормализация через z-оценку идентифицирует значения, отклоняющиеся на заданное количество стандартных отклонений от среднего
    FOREACH rec IN ARRAY v_target_cols LOOP
        IF v_partition_cols IS NULL THEN
            v_sql := FORMAT(
                'INSERT INTO %1$I
                 SELECT
                   %2$s AS group_key,
                   t.%3$I AS ts,
                   %4$L AS target_col,
                   t.%4$I::numeric AS value,
                   AVG(t.%4$I::numeric) OVER (ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING),
                   STDDEV_SAMP(t.%4$I::numeric) OVER (ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING),
                   CASE
                     WHEN STDDEV_SAMP(t.%4$I::numeric) OVER (ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING) = 0
                     THEN NULL
                     ELSE
                       (t.%4$I::numeric -
                        AVG(t.%4$I::numeric) OVER (ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING))
                       /
                       STDDEV_SAMP(t.%4$I::numeric) OVER (ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING)
                   END
                 FROM %6$I.%7$I t
                 WHERE t.%4$I IS NOT NULL',
                v_tmp_table,
                v_group_expr,
                v_time_col,
                rec,
                v_window_size,
                p_schema,
                p_table
            );
        ELSE
            v_sql := FORMAT(
                'INSERT INTO %1$I
                 SELECT
                   %2$s AS group_key,
                   t.%3$I AS ts,
                   %4$L AS target_col,
                   t.%4$I::numeric AS value,
                   AVG(t.%4$I::numeric) OVER (PARTITION BY %8$s ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING),
                   STDDEV_SAMP(t.%4$I::numeric) OVER (PARTITION BY %8$s ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING),
                   CASE
                     WHEN STDDEV_SAMP(t.%4$I::numeric) OVER (PARTITION BY %8$s ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING) = 0
                     THEN NULL
                     ELSE
                       (t.%4$I::numeric -
                        AVG(t.%4$I::numeric) OVER (PARTITION BY %8$s ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING))
                       /
                       STDDEV_SAMP(t.%4$I::numeric) OVER (PARTITION BY %8$s ORDER BY t.%3$I ROWS BETWEEN %5$s PRECEDING AND 1 PRECEDING)
                   END
                 FROM %6$I.%7$I t
                 WHERE t.%4$I IS NOT NULL',
                v_tmp_table,
                v_group_expr,
                v_time_col,
                rec,
                v_window_size,
                p_schema,
                p_table,
                v_partition_cols
            );
        END IF;

        EXECUTE v_sql;
    END LOOP;

    -- подсчет количества групп с аномалиями и общего числа выявленных аномальных значений
    -- результат сравнивает абсолютное значение z-оценки с установленным порогом для идентификации выбросов
    EXECUTE FORMAT(
        'SELECT COUNT(DISTINCT group_key), COUNT(*) FROM %I WHERE z_score IS NOT NULL AND ABS(z_score) > %s',
        v_tmp_table, v_z_threshold
    ) INTO v_groups, v_anoms;

    -- обновление аудит-записи с результатами анализа, включая количество обработанных групп, параметры и общее число выявленных аномалий
    UPDATE dedup_audit
    SET groups_processed = v_groups,
        details = JSONB_BUILD_OBJECT(
            'window_size', v_window_size,
            'z_threshold', v_z_threshold,
            'targets', v_target_cols,
            'anomalies', v_anoms
        )
    WHERE id = v_audit_id;

    RETURN JSONB_BUILD_OBJECT(
        'audit_id', v_audit_id,
        'groups', v_groups,
        'anomalies', v_anoms
    );
END;
$$;


CREATE OR REPLACE FUNCTION anomaly_fix_timeseries(
    p_schema TEXT,
    p_table TEXT,
    p_target_columns TEXT[] DEFAULT NULL,
    p_key_cols TEXT[] DEFAULT NULL,
    p_action TEXT DEFAULT 'replace_with_rolling_mean',
    p_params JSONB DEFAULT '{}'::JSONB,
    p_dry_run BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_time_col TEXT;
    v_window_size INT := COALESCE((p_params->>'window_size')::INT, 7);
    v_z_threshold NUMERIC := COALESCE((p_params->>'z_threshold')::NUMERIC, 3.0);
    v_target_cols TEXT[];
    v_partition_cols TEXT;
    v_tmp_fix TEXT;
    v_sql TEXT;
    v_audit_id INT;
    v_fixed BIGINT := 0;
    rec TEXT;
BEGIN
    -- выделение имени столбца времени из параметров для использования в оконных функциях
    v_time_col := p_params->>'time_column';
    IF v_time_col IS NULL THEN
        RAISE EXCEPTION 'p_params must contain time_column';
    END IF;

    -- автоматическое определение целевых числовых столбцов из информационной схемы базы данных
    -- при отсутствии явно переданного списка анализируются все колонки с числовыми типами
    IF p_target_columns IS NULL THEN
        SELECT array_agg(column_name::TEXT)
        INTO v_target_cols
        FROM information_schema.columns
        WHERE table_schema = p_schema
          AND table_name = p_table
          AND data_type IN (
              'smallint','integer','bigint',
              'numeric','real','double precision','decimal'
          )
          AND column_name <> v_time_col
          AND (p_key_cols IS NULL OR column_name <> ALL(p_key_cols));
    ELSE
        v_target_cols := p_target_columns;
    END IF;

    IF v_target_cols IS NULL OR array_length(v_target_cols,1) = 0 THEN
        RAISE EXCEPTION 'не найдено колонок для исправления';
    END IF;

    -- логирование инициирования исправления в аудит-таблице с сохранением параметров вызова
    -- запись включает временную метку, схему, таблицу, указанные ключевые колонки и выбранное действие
    INSERT INTO dedup_audit(
        run_ts, db_schema, db_table, key_cols,
        action, dry_run, groups_processed, details
    )
    VALUES (
        NOW(), p_schema, p_table, p_key_cols,
        'fix_timeseries', p_dry_run, 0,
        JSONB_BUILD_OBJECT('action', p_action)
    )
    RETURNING id INTO v_audit_id;

    v_tmp_fix := FORMAT('__fix_ts_%s', v_audit_id);

    -- инициализация временной таблицы для накопления информации об исправлениях
    -- таблица хранит идентификаторы строк, названия столбцов, старые значения, новые значения и соответствующие z-оценки
    EXECUTE FORMAT(
        'CREATE TEMP TABLE %I (
            row_ctid TID,
            target_col TEXT,
            old_value NUMERIC,
            new_value NUMERIC,
            z_score NUMERIC
        ) ON COMMIT DROP',
        v_tmp_fix
    );

    -- формирование выражений для логического разделения данных по ключевым столбцам
    -- при отсутствии ключей используется глобальная группа для обработки всего датасета единой аналитикой
    IF p_key_cols IS NULL OR array_length(p_key_cols,1) = 0 THEN
        v_partition_cols := NULL;
    ELSE
        SELECT string_agg(FORMAT('t.%I', k), ', ')
        INTO v_partition_cols
        FROM unnest(p_key_cols) AS k;
    END IF;

    -- идентификация аномальных значений в каждом целевом столбце и подготовка к их исправлению
    -- формирование списка строк для замены с расчетом новых значений на основе скользящего среднего
    FOREACH rec IN ARRAY v_target_cols LOOP
        IF v_partition_cols IS NULL THEN
            v_sql := FORMAT(
                'INSERT INTO %1$I
                 SELECT
                   ctid AS row_ctid,
                   %2$L AS target_col,
                   value AS old_value,
                   rolling_mean AS new_value,
                   z_score
                 FROM (
                   SELECT
                     t.ctid,
                     t.%2$I::numeric AS value,
                     AVG(t.%2$I::numeric) OVER (
                         ORDER BY t.%3$I
                         ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                     ) AS rolling_mean,
                     STDDEV_SAMP(t.%2$I::numeric) OVER (
                         ORDER BY t.%3$I
                         ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                     ) AS rolling_std,
                     (t.%2$I::numeric -
                      AVG(t.%2$I::numeric) OVER (
                          ORDER BY t.%3$I
                          ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                      ))
                     /
                     NULLIF(
                         STDDEV_SAMP(t.%2$I::numeric) OVER (
                             ORDER BY t.%3$I
                             ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                         ),
                         0
                     ) AS z_score
                   FROM %5$I.%6$I t
                   WHERE t.%2$I IS NOT NULL
                 ) s
                 WHERE ABS(z_score) > %7$s',
                v_tmp_fix,
                rec,
                v_time_col,
                v_window_size,
                p_schema,
                p_table,
                v_z_threshold
            );
        ELSE
            v_sql := FORMAT(
                'INSERT INTO %1$I
                 SELECT
                   ctid AS row_ctid,
                   %2$L AS target_col,
                   value AS old_value,
                   rolling_mean AS new_value,
                   z_score
                 FROM (
                   SELECT
                     t.ctid,
                     t.%2$I::numeric AS value,
                     AVG(t.%2$I::numeric) OVER (
                         PARTITION BY %8$s
                         ORDER BY t.%3$I
                         ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                     ) AS rolling_mean,
                     STDDEV_SAMP(t.%2$I::numeric) OVER (
                         PARTITION BY %8$s
                         ORDER BY t.%3$I
                         ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                     ) AS rolling_std,
                     (t.%2$I::numeric -
                      AVG(t.%2$I::numeric) OVER (
                          PARTITION BY %8$s
                          ORDER BY t.%3$I
                          ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                      ))
                     /
                     NULLIF(
                         STDDEV_SAMP(t.%2$I::numeric) OVER (
                             PARTITION BY %8$s
                             ORDER BY t.%3$I
                             ROWS BETWEEN %4$s PRECEDING AND 1 PRECEDING
                         ),
                         0
                     ) AS z_score
                   FROM %5$I.%6$I t
                   WHERE t.%2$I IS NOT NULL
                 ) s
                 WHERE ABS(z_score) > %7$s',
                v_tmp_fix,
                rec,
                v_time_col,
                v_window_size,
                p_schema,
                p_table,
                v_z_threshold,
                v_partition_cols
            );
        END IF;

        EXECUTE v_sql;
    END LOOP;

    -- применение исправлений к исходной таблице при отключенном режиме пробного запуска
    -- осуществляется обновление столбцов с новыми значениями по всем выявленным аномальным строкам
    IF NOT p_dry_run THEN
        FOREACH rec IN ARRAY v_target_cols LOOP
            EXECUTE FORMAT(
                'UPDATE %1$I.%2$I t
                 SET %3$I = f.new_value
                 FROM %4$I f
                 WHERE f.target_col = %5$L
                   AND t.ctid = f.row_ctid',
                p_schema,
                p_table,
                rec,
                v_tmp_fix,
                rec
            );
        END LOOP;
    END IF;

    EXECUTE FORMAT('SELECT COUNT(*) FROM %I', v_tmp_fix)
    INTO v_fixed;

    -- обновление аудит-записи с результатами исправления, включая количество обработанных строк, выбранное действие и статус пробного запуска
    UPDATE dedup_audit
    SET groups_processed = v_fixed,
        details = JSONB_BUILD_OBJECT(
            'fixed_rows', v_fixed,
            'action', p_action,
            'dry_run', p_dry_run
        )
    WHERE id = v_audit_id;

    RETURN JSONB_BUILD_OBJECT(
        'audit_id', v_audit_id,
        'fixed_rows', v_fixed,
        'dry_run', p_dry_run
    );
END;
$$;
