-- Ingest Control Table
CREATE TABLE IF NOT EXISTS ingest_control (
    id SERIAL PRIMARY KEY,
    ip_address VARCHAR(50) NOT NULL,
    port INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    chunk_size INTEGER DEFAULT 100,
    max_chunks_per_run INTEGER DEFAULT 20, -- max chunks to process in single run for backfill
    active BOOLEAN DEFAULT false,
    endpoint_type VARCHAR(50) NOT NULL, -- 'mysql', 'postgres', 'file'
    database_name VARCHAR(100), -- for database endpoints
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Accelerometer data table (from MySQL endpoint)
CREATE TABLE IF NOT EXISTS accelerometer_data (
    id SERIAL PRIMARY KEY,
    endpoint_name VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    accel_x DECIMAL(10, 6),
    accel_y DECIMAL(10, 6),
    accel_z DECIMAL(10, 6),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_id INTEGER -- original ID from source system
);

-- Accelerometer + Magnetometer data table (from PostgreSQL endpoint)
CREATE TABLE IF NOT EXISTS accel_mag_data (
    id SERIAL PRIMARY KEY,
    endpoint_name VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    accel_x DECIMAL(10, 6),
    accel_y DECIMAL(10, 6),
    accel_z DECIMAL(10, 6),
    mag_x DECIMAL(10, 6),
    mag_y DECIMAL(10, 6),
    mag_z DECIMAL(10, 6),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_id INTEGER
);

-- File metadata table (from file endpoint)
CREATE TABLE IF NOT EXISTS file_metadata (
    id SERIAL PRIMARY KEY,
    endpoint_name VARCHAR(255) NOT NULL,
    folder_path VARCHAR(500) NOT NULL,
    xml_file VARCHAR(255),
    kmz_file VARCHAR(255),
    image_count INTEGER,
    created_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample ingest control records
INSERT INTO ingest_control (ip_address, port, name, chunk_size, max_chunks_per_run, active, endpoint_type, database_name) VALUES
('mysql-endpoint', 3306, 'MySQL Accelerometer Sensor', 50, 20, true, 'mysql', 'sensors'),
('postgres-endpoint', 5432, 'PostgreSQL Accel+Mag Sensor', 50, 20, true, 'postgres', 'sensors'),
('file-endpoint', 8000, 'File-based Camera System', 10, 50, true, 'file', NULL);

-- Create indexes for better query performance
CREATE INDEX idx_accelerometer_timestamp ON accelerometer_data(timestamp);
CREATE INDEX idx_accelerometer_endpoint ON accelerometer_data(endpoint_name);
CREATE INDEX idx_accel_mag_timestamp ON accel_mag_data(timestamp);
CREATE INDEX idx_accel_mag_endpoint ON accel_mag_data(endpoint_name);
CREATE INDEX idx_file_metadata_endpoint ON file_metadata(endpoint_name);
CREATE INDEX idx_ingest_control_active ON ingest_control(active);
