CREATE TABLE IF NOT EXISTS sensor_readings (
    id          BIGSERIAL PRIMARY KEY,
    sensor_id   VARCHAR(50)      NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    event_time  TIMESTAMPTZ      NOT NULL,
    is_anomaly  BOOLEAN          NOT NULL,
    created_at  TIMESTAMPTZ      DEFAULT CURRENT_TIMESTAMP
);

-- Optional: indexes that would speed up the API queries as data grows.
-- CREATE INDEX IF NOT EXISTS idx_sensor_readings_event_time ON sensor_readings (event_time DESC);
-- CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_id ON sensor_readings (sensor_id);
