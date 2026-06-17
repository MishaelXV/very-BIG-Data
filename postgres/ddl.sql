-- Idempotent: safe to run multiple times
CREATE TABLE IF NOT EXISTS device_types (
    id          INTEGER         PRIMARY KEY,
    type_name   VARCHAR(100)    NOT NULL,
    description VARCHAR(255),
    created_at  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);
