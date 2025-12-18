-- шаг 1: обнаружение пропусков с большим лимитом выборки для отчета
SELECT anomaly_detect_missing(
  p_schema         := 'public',
  p_table          := 'power_consumption',
  p_target_columns := ARRAY['global_active_power','global_reactive_power','voltage','global_intensity','sub_metering_1','sub_metering_2','sub_metering_3'],
  p_key_cols       := ARRAY['id'],
  p_params         := '{"limit_sample":1048575}'::jsonb,
  p_dry_run        := true
);

-- шаг 2: пробный сценарий исправления пропусков без применения изменений (dry run)
-- пояснение: вывод планируемых действий и объем затронутых строк без модификации данных
SELECT anomaly_fix_missing(
  p_schema         := 'public',
  p_table          := 'power_consumption',
  p_target_columns := NULL,
  p_key_cols       := ARRAY['id'],
  p_action         := NULL,
  p_params         := '{
        "actions": {
            "global_active_power":   {"method":"delete_row"},
            "global_reactive_power": {"method":"set_constant","value":0},
            "voltage":               {"method":"set_mode"},
            "global_intensity":      {"method":"set_mean"},
            "sub_metering_1":        {"method":"set_median"},
            "sub_metering_2":        {"method":"forward_fill", "order_by":"global_intensity"},
            "sub_metering_3":        {"method":"backward_fill", "order_by":"voltage"}
        }
  }'::jsonb,
  p_dry_run        := true
);

-- шаг 3: применение исправлений пропусков
-- пояснение: использование разных стратегий для колонок, сухой прогон отключен, изменения вносятся
SELECT anomaly_fix_missing(
  p_schema         := 'public',
  p_table          := 'power_consumption',
  p_target_columns := NULL,
  p_key_cols       := ARRAY['id'],
  p_action         := NULL,
  p_params         := '{
    "actions": {
        "global_active_power":   {"method":"delete_row"},
        "global_reactive_power": {"method":"set_constant","value":0},
        "voltage":               {"method":"set_mode"},
        "global_intensity":      {"method":"set_mean"},
        "sub_metering_1":        {"method":"set_median"},
        "sub_metering_2":        {"method":"forward_fill","order_by":"global_intensity"},
        "sub_metering_3":        {"method":"copy_from_other_column","source_column":"sub_metering_2"}
    }
  }'::jsonb,
  p_dry_run        := false
);

-- шаг 4: обнаружение дубликатов по полному набору показателей и времени измерения
SELECT anomaly_detect_duplicates(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['measurement_timestamp','global_active_power','global_reactive_power','voltage','global_intensity','sub_metering_1','sub_metering_2','sub_metering_3'],
    p_key_cols       := NULL,
    p_params         := '{"sample_limit":5}'::jsonb,
    p_dry_run        := true
);

-- шаг 5: удаление дубликатов с сохранением первой записи в каждой группе
-- пояснение: при p_dry_run=false выполняется удаление строк, аудит сохраняет детали
SELECT anomaly_fix_duplicates(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['measurement_timestamp','global_active_power','global_reactive_power','voltage','global_intensity','sub_metering_1','sub_metering_2','sub_metering_3'],
    p_key_cols       := NULL,
    p_action         := 'delete',
    p_params         := '{"keep":"first"}'::jsonb,
    p_dry_run        := false
);

-- шаг 6: обнаружение выбросов по глобальной активной мощности методом iqr
-- пояснение: p_dry_run=true значит только логирование, без изменений
SELECT anomaly_detect_outliers(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_active_power'],
    p_key_cols       := ARRAY['id'],
    p_params         := '{"method":"iqr","k":1.5}'::jsonb,
    p_dry_run        := true
);

-- шаг 7: фиксация выбросов по напряжению методом zscore, установка флага в отдельную колонку
-- пояснение: добавление колонки voltage_outlier при необходимости, далее выставление флага
SELECT anomaly_fix_outliers(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['voltage'],
    p_key_cols       := ARRAY['id'],
    p_action         := 'flag',
    p_params         := '{"method":"zscore","threshold":3,"flag_column":"voltage_outlier"}'::jsonb,
    p_dry_run        := false
);

-- шаг 8: фиксация выбросов по реактивной мощности методом mad, замена на медиану
SELECT anomaly_fix_outliers(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_reactive_power'],
    p_key_cols       := ARRAY['id'],
    p_action         := 'replace_with_median',
    p_params         := '{"method":"mad","threshold":3.5}'::jsonb,
    p_dry_run        := false
);

-- шаг 9: фиксация выбросов по глобальной силе тока методом iqr с ограничением значений
-- пояснение: обрезание значений, превышающих границы межквартильного диапазона, приведение их к максимально допустимым пределам
SELECT anomaly_fix_outliers(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_intensity'],
    p_key_cols       := ARRAY['id'],
    p_action         := 'cap',
    p_params         := '{"method":"iqr","k":1.5}'::jsonb,
    p_dry_run        := false
);

-- шаг 10: обнаружение выбросов по sub_metering_1 методом mad, только логирование
-- пояснение: выявление аномалий с использованием абсолютного отклонения от медианы без реальной обработки данных
SELECT anomaly_detect_outliers(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['sub_metering_1'],
    p_key_cols       := ARRAY['id'],
    p_params         := '{"method":"mad","threshold":3.5}'::jsonb,
    p_dry_run        := true
);

-- шаг 11: обнаружение выбросов по sub_metering_2 методом mad, только логирование
-- пояснение:выявление аномалий с использованием абсолютного отклонения от медианы без реальной обработки данных
SELECT anomaly_detect_outliers(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['sub_metering_2'],
    p_key_cols       := ARRAY['id'],
    p_params         := '{"method":"mad","threshold":3.5}'::jsonb,
    p_dry_run        := true
);

-- шаг 12: обнаружение аномалий на основе правил (dry-run)
-- пояснение: проверка строк на соответствие заданным sql-условиям, логирование нарушений без изменений
SELECT anomaly_detect_rule_based(
    'public',
    'power_consumption',
    NULL,
    ARRAY['id'],
    '{
        "rules": [
            {
                "name": "neg_global_active_power",
                "expr": "global_active_power < 0",
                "severity": "high",
                "description": "глобальная активная мощность не может быть отрицательной"
            },
            {
                "name": "neg_global_reactive_power",
                "expr": "global_reactive_power < 0",
                "severity": "high",
                "description": "глобальная реактивная мощность не может быть отрицательной"
            },
            {
                "name": "voltage_out_of_range",
                "expr": "voltage < 200 OR voltage > 250",
                "severity": "medium",
                "description": "напряжение должно быть в диапазоне 200-250 в"
            },
            {
                "name": "global_intensity_too_high",
                "expr": "global_intensity > 100",
                "severity": "low",
                "description": "глобальная сила тока слишком велика"
            }
        ]
    }'::jsonb,
    TRUE
);

-- шаг 13: исправление аномалий на основе правил (dry-run)
-- пояснение: планирование действий для строк с нарушениями, вывод в аудит без модификации данных
SELECT anomaly_fix_rule_based(
    'public',
    'power_consumption',
    NULL,
    ARRAY['id'],
    NULL,
    '{
        "rules": [
            {
                "name": "neg_global_active_power",
                "expr": "global_active_power < 0",
                "action": "set_value",
                "params": {"set_value": {"column":"global_active_power", "value":"0"}}
            },
            {
                "name": "neg_global_reactive_power",
                "expr": "global_reactive_power < 0",
                "action": "set_value",
                "params": {"set_value": {"column":"global_reactive_power", "value":"0"}}
            },
            {
                "name": "voltage_out_of_range",
                "expr": "voltage < 200 OR voltage > 250",
                "action": "set_null",
                "params": {"target_columns":["voltage"]}
            },
            {
                "name": "global_intensity_too_high",
                "expr": "global_intensity > 100",
                "action": "set_null",
                "params": {"target_columns":["global_intensity"]}
            }
        ]
    }'::jsonb,
    TRUE
);

-- шаг 14: исправление аномалий на основе правил (выполнение изменений)
-- пояснение: применение действий к строкам с нарушениями, реальная модификация данных
SELECT anomaly_fix_rule_based(
    'public',
    'power_consumption',
    NULL,
    ARRAY['id'],
    NULL,
    '{
        "rules": [
            {
                "name": "neg_global_active_power",
                "expr": "global_active_power < 0",
                "action": "set_value",
                "params": {"set_value": {"column":"global_active_power", "value":"0"}}
            },
            {
                "name": "neg_global_reactive_power",
                "expr": "global_reactive_power < 0",
                "action": "set_value",
                "params": {"set_value": {"column":"global_reactive_power", "value":"0"}}
            },
            {
                "name": "voltage_out_of_range",
                "expr": "voltage < 200 OR voltage > 250",
                "action": "set_null",
                "params": {"target_columns":["voltage"]}
            },
            {
                "name": "global_intensity_too_high",
                "expr": "global_intensity > 100",
                "action": "set_null",
                "params": {"target_columns":["global_intensity"]}
            }
        ]
    }'::jsonb,
    FALSE
);

-- шаг 15: базовое обнаружение аномалий по всем числовым колонкам
-- пояснение: выявление временных аномалий во всех числовых столбцах таблицы с использованием скользящего окна анализа
SELECT anomaly_detect_timeseries(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := NULL,
    p_key_cols       := NULL,
    p_params         := '{"time_column":"measurement_timestamp"}'::JSONB,
    p_dry_run        := TRUE
);

-- шаг 16: обнаружение аномалий только по активной мощности и напряжению
-- пояснение: выявление временных аномалий в двух целевых столбцах с увеличенным размером окна и пониженным порогом чувствительности
SELECT anomaly_detect_timeseries(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_active_power','voltage'],
    p_key_cols       := NULL,
    p_params         := '{
        "time_column":"measurement_timestamp",
        "window_size":14,
        "z_threshold":2.5
    }'::JSONB,
    p_dry_run        := TRUE
);

-- шаг 17:обнаружение аномалий с группировкой по id
-- пояснение: выявление временных аномалий отдельно для каждого идентификатора с уменьшенным размером анализируемого окна
SELECT anomaly_detect_timeseries(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_active_power'],
    p_key_cols       := ARRAY['id'],
    p_params         := '{
        "time_column":"measurement_timestamp",
        "window_size":7,
        "z_threshold":3
    }'::JSONB,
    p_dry_run        := TRUE
);

-- шаг 18: замена аномальных значений на скользящее среднее
-- пояснение: замещение выявленных выбросов значениями скользящего среднего для сглаживания экстремальных отклонений
SELECT anomaly_fix_timeseries(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_active_power'],
    p_key_cols       := NULL,
    p_action         := 'replace_with_mean',
    p_params         := '{
        "time_column":"measurement_timestamp",
        "window_size":7,
        "z_threshold":3
    }'::JSONB,
    p_dry_run        := FALSE
);

-- шаг 19: замена аномальных значений установкой NULL (без реального изменения данных)
-- пояснение: выявление аномальных значений и планирование их обнуления в пробном режиме без применения изменений
SELECT anomaly_fix_timeseries(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['voltage'],
    p_key_cols       := NULL,
    p_action         := 'nullify',
    p_params         := '{
        "time_column":"measurement_timestamp",
        "window_size":10,
        "z_threshold":2.8
    }'::JSONB,
    p_dry_run        := TRUE
);

-- шаг 20: удаление строк с экстремальными аномалиями
-- пояснение: исключение из таблицы строк с очень экстремальными выбросами, превышающими четырехсигма-отклонение
SELECT anomaly_fix_timeseries(
    p_schema         := 'public',
    p_table          := 'power_consumption',
    p_target_columns := ARRAY['global_intensity'],
    p_key_cols       := NULL,
    p_action         := 'delete',
    p_params         := '{
        "time_column":"measurement_timestamp",
        "window_size":5,
        "z_threshold":4
    }'::JSONB,
    p_dry_run        := FALSE
);
