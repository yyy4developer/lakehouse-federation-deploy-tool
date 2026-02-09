CREATE TABLE IF NOT EXISTS public.sensor_readings (
    reading_id      INT PRIMARY KEY,
    sensor_id       INT NOT NULL,
    machine_id      INT NOT NULL,
    reading_time    TIMESTAMP NOT NULL,
    value           DECIMAL(10,2) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'normal'
)
