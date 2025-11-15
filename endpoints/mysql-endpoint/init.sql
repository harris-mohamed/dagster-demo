USE sensors;

CREATE TABLE IF NOT EXISTS measurements (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    accel_x DECIMAL(10, 6),
    accel_y DECIMAL(10, 6),
    accel_z DECIMAL(10, 6),
    INDEX idx_timestamp (timestamp)
);
