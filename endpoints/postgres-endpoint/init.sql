CREATE TABLE IF NOT EXISTS measurements (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    accel_x DECIMAL(10, 6),
    accel_y DECIMAL(10, 6),
    accel_z DECIMAL(10, 6),
    mag_x DECIMAL(10, 6),
    mag_y DECIMAL(10, 6),
    mag_z DECIMAL(10, 6)
);

CREATE INDEX idx_measurements_timestamp ON measurements(timestamp);
