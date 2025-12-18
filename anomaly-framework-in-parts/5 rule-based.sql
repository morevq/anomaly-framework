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
    INSERT INTO public.dedup_audit(db_schema, db_table, key_cols, action, dry_run, groups_processed, details)
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
        INSERT INTO public.dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
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
    UPDATE public.dedup_audit
    SET groups_processed = (SELECT COUNT(*) FROM public.dedup_audit_rows WHERE audit_id = v_audit_id),
        details = JSONB_BUILD_OBJECT(
            'rules_count', JSONB_ARRAY_LENGTH(v_rules),
            'rules', v_rules_summary
        )
    WHERE id = v_audit_id;

    RETURN JSONB_BUILD_OBJECT(
        'status','ok',
        'audit_id', v_audit_id,
        'details', (SELECT details FROM public.dedup_audit WHERE id = v_audit_id)
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
    INSERT INTO public.dedup_audit(db_schema, db_table, key_cols, action, dry_run, groups_processed, details)
    VALUES (p_schema, p_table, p_key_cols, 'fix_rule_based', p_dry_run, 0, '{}'::JSONB)
    RETURNING id INTO v_audit_id;

    -- обработка правил
    FOR v_rule IN SELECT * FROM jsonb_array_elements(v_rules)
    LOOP
        v_idx := v_idx + 1;

        IF (v_rule->>'expr') IS NULL THEN
            INSERT INTO public.dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
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
            INSERT INTO public.dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
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
            INSERT INTO public.dedup_audit_rows(audit_id, group_key, kept_ctid, removed_ctids, note, extra)
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
    UPDATE public.dedup_audit
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
        UPDATE public.dedup_audit
        SET details = JSONB_BUILD_OBJECT('error', SQLSTATE, 'message', SQLERRM)
        WHERE id = v_audit_id;
        RAISE;
END;
$$;
