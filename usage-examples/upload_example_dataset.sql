-- подготовка временной таблицы для загрузки csv, хранение строковых значений как есть
CREATE TABLE temp_power_data (
    "Date" TEXT,
    "Time" TEXT,
    "Global_active_power" TEXT,
    "Global_reactive_power" TEXT,
    "Voltage" TEXT,
    "Global_intensity" TEXT,
    "Sub_metering_1" TEXT,
    "Sub_metering_2" TEXT,
    "Sub_metering_3" TEXT
);

-- импорт csv в temp_power_data

-- итоговая таблица фактов с типами для дальнейшей аналитики
CREATE TABLE IF NOT EXISTS power_consumption (
    id SERIAL PRIMARY KEY,
    measurement_timestamp TIMESTAMP NOT NULL,
    global_active_power NUMERIC(10, 3),
    global_reactive_power NUMERIC(10, 3),
    voltage NUMERIC(10, 3),
    global_intensity NUMERIC(10, 3),
    sub_metering_1 NUMERIC(10, 3),
    sub_metering_2 NUMERIC(10, 3),
    sub_metering_3 NUMERIC(10, 3)
);

-- перенос данных из временной таблицы в основную, преобразование типов, замена '?' на null
INSERT INTO power_consumption (
    measurement_timestamp,
    global_active_power,
    global_reactive_power,
    voltage,
    global_intensity,
    sub_metering_1,
    sub_metering_2,
    sub_metering_3
)
SELECT 
    TO_TIMESTAMP("Date" || ' ' || "Time", 'DD.MM.YYYY HH24:MI:SS'),
    NULLIF("Global_active_power", '?')::NUMERIC(10, 3),
    NULLIF("Global_reactive_power", '?')::NUMERIC(10, 3),
    NULLIF("Voltage", '?')::NUMERIC(10, 3),
    NULLIF("Global_intensity", '?')::NUMERIC(10, 3),
    NULLIF("Sub_metering_1", '?')::NUMERIC(10, 3),
    NULLIF("Sub_metering_2", '?')::NUMERIC(10, 3),
    NULLIF("Sub_metering_3", '?')::NUMERIC(10, 3)
FROM temp_power_data;

-- быстрая проверка корректности загрузки
SELECT * FROM power_consumption LIMIT 10;