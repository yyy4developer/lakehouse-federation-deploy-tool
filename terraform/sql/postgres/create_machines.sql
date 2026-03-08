CREATE TABLE IF NOT EXISTS machines (
    machine_id      INTEGER PRIMARY KEY,
    machine_name    VARCHAR(80) NOT NULL,
    production_line VARCHAR(40) NOT NULL,
    factory         VARCHAR(40) NOT NULL,
    status          VARCHAR(20) NOT NULL
);
