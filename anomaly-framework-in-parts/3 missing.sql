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