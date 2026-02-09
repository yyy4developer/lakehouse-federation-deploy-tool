CREATE TABLE IF NOT EXISTS public.production_events (
    event_id          INT PRIMARY KEY,
    machine_id        INT NOT NULL,
    event_type        VARCHAR(30) NOT NULL,
    event_time        TIMESTAMP NOT NULL,
    duration_minutes  INT,
    description       VARCHAR(200)
)
