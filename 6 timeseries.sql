CREATE OR REPLACE FUNCTION public.anomaly_detect_timeseries(
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
    INSERT INTO public.dedup_audit(
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
    UPDATE public.dedup_audit
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


CREATE OR REPLACE FUNCTION public.anomaly_fix_timeseries(
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
    INSERT INTO public.dedup_audit(
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
    UPDATE public.dedup_audit
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
