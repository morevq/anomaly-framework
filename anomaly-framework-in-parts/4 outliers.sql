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
    SELECT format_type(a.atttypid, a.atttymod)
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
