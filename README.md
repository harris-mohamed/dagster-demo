# Dagster ETL Sensor Demo

A fully dockerized demonstration of Dagster as an ETL orchestration platform with sensor-based monitoring. This project showcases how Dagster can monitor a control table and dynamically ingest data from multiple unstable endpoints into a central database.

## Quick Start

```bash
# Build and start all containers
docker-compose up --build

# Access Dagster UI at http://localhost:3000
# The sensor will automatically start monitoring and ingesting data!

# View logs
docker-compose logs -f dagster
docker-compose logs -f mysql-generator
docker-compose logs -f postgres-generator
docker-compose logs -f file-endpoint
```

## Architecture Overview

### Components

1. **Supabase (Central Database)**
   - PostgreSQL-based central data warehouse
   - Contains `ingest_control` table for dynamic endpoint configuration
   - Stores ingested data from all endpoints
   - Port: 5432

2. **Endpoint 1: MySQL Database + Generator**
   - MySQL database storing accelerometer data (X, Y, Z)
   - Separate Python container generating data every 10 seconds
   - Simulates an unstable sensor endpoint
   - Port: 3306

3. **Endpoint 2: PostgreSQL Database + Generator**
   - PostgreSQL database storing accelerometer + magnetometer data
   - Separate Python container generating data every 10 seconds
   - Simulates a combo sensor chip
   - Port: 5433

4. **Endpoint 3: File-based System**
   - Generates folders with XML, KMZ, and image files every 10 seconds
   - Simulates a camera/GPS system
   - Serves files via HTTP on port 8000

5. **Dagster**
   - Orchestrates ETL pipelines
   - Monitors `ingest_control` table via sensor
   - Dynamically creates jobs for active endpoints
   - Web UI: http://localhost:3000

## Data Flow

```
┌─────────────────┐
│ ingest_control  │  ← Dagster Sensor monitors this table
└─────────────────┘
        │
        ├─→ Active: MySQL Endpoint → Dagster ETL → accelerometer_data
        ├─→ Active: Postgres Endpoint → Dagster ETL → accel_mag_data
        └─→ Active: File Endpoint → Dagster ETL → file_metadata
```

## Quick Start

### Prerequisites
- Docker
- Docker Compose

### 1. Start All Services

```bash
docker-compose up --build
```

This will start:
- Supabase database (initializes with control table and data tables)
- MySQL endpoint (starts generating data)
- PostgreSQL endpoint (starts generating data)
- File endpoint (starts generating files)
- Dagster (web server + daemon with sensor)

### 2. Access Dagster UI

Navigate to http://localhost:3000

You should see:
- **Assets**: `ingest_mysql_data`, `ingest_postgres_data`, `ingest_file_data`
- **Sensors**: `endpoint_monitor_sensor` (should be running)
- **Jobs**: `etl_job`

### 3. Monitor ETL Activity

The sensor checks the `ingest_control` table every 30 seconds. When it finds active endpoints, it automatically triggers ETL jobs.

Watch the Dagster UI to see:
- Sensor evaluations in the "Sensors" tab
- Runs being triggered automatically
- Asset materializations showing ingestion progress

## Database Schema

### ingest_control (Supabase)

Controls which endpoints are actively being monitored and ingested:

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| ip_address | VARCHAR(50) | Hostname or IP of endpoint |
| port | INTEGER | Port number |
| name | VARCHAR(255) | Human-readable name |
| chunk_size | INTEGER | Number of records to ingest per run |
| active | BOOLEAN | Enable/disable ingestion |
| endpoint_type | VARCHAR(50) | 'mysql', 'postgres', or 'file' |
| database_name | VARCHAR(100) | Database name (for DB endpoints) |

### accelerometer_data (Supabase)

Stores data from MySQL endpoint:

```sql
timestamp, accel_x, accel_y, accel_z, endpoint_name, source_id
```

### accel_mag_data (Supabase)

Stores data from PostgreSQL endpoint:

```sql
timestamp, accel_x, accel_y, accel_z, mag_x, mag_y, mag_z, endpoint_name, source_id
```

### file_metadata (Supabase)

Stores metadata from file endpoint:

```sql
folder_path, xml_file, kmz_file, image_count, created_at, endpoint_name
```

## Configuration

### Enable/Disable Endpoints

Connect to Supabase and toggle the `active` flag:

```bash
# Access Supabase container
docker exec -it supabase-db psql -U postgres -d postgres

# Disable MySQL endpoint
UPDATE ingest_control SET active = false WHERE endpoint_type = 'mysql';

# Enable it again
UPDATE ingest_control SET active = true WHERE endpoint_type = 'mysql';

# Adjust chunk size
UPDATE ingest_control SET chunk_size = 100 WHERE name = 'MySQL Accelerometer Sensor';
```

### Add New Endpoints

```sql
INSERT INTO ingest_control (ip_address, port, name, chunk_size, active, endpoint_type, database_name)
VALUES ('new-mysql-server', 3306, 'New Sensor', 50, true, 'mysql', 'sensors');
```

## How It Works

### Sensor-Based Architecture

1. **Sensor Polling**: The `endpoint_monitor_sensor` runs every 30 seconds
2. **Control Table Query**: Sensor queries `ingest_control` for active endpoints
3. **Dynamic Job Creation**: For each active endpoint, sensor creates a `RunRequest`
4. **Asset Selection**: Sensor selects the appropriate asset based on `endpoint_type`
5. **ETL Execution**: Dagster executes the selected asset with endpoint configuration
6. **Incremental Loading**: Each asset tracks the last ingested ID/folder to avoid duplicates

### Error Handling

- Endpoints may be unstable (by design)
- Dagster retries failed runs automatically
- Sensor continues monitoring even if some endpoints fail
- Each endpoint is processed independently

### Chunking

- Controlled by `chunk_size` in `ingest_control` table
- Prevents overwhelming the system with large datasets
- Each run ingests up to `chunk_size` records
- Sensor triggers again on next evaluation if more data exists

## Monitoring & Debugging

### View Endpoint Data Generation

```bash
# MySQL endpoint logs
docker logs -f mysql-endpoint

# PostgreSQL endpoint logs
docker logs -f postgres-endpoint

# File endpoint logs
docker logs -f file-endpoint

# Dagster logs
docker logs -f dagster
```

### Query Ingested Data

```bash
docker exec -it supabase-db psql -U postgres -d postgres

# Count ingested records
SELECT endpoint_name, COUNT(*) FROM accelerometer_data GROUP BY endpoint_name;
SELECT endpoint_name, COUNT(*) FROM accel_mag_data GROUP BY endpoint_name;
SELECT endpoint_name, COUNT(*) FROM file_metadata GROUP BY endpoint_name;

# View recent ingestions
SELECT * FROM accelerometer_data ORDER BY ingested_at DESC LIMIT 10;
```

### Inspect Generated Files

```bash
# List generated folders
docker exec file-endpoint ls -la /data

# View a specific folder
docker exec file-endpoint ls -la /data/20250114_120000
```

## Cleanup

```bash
# Stop all containers
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v
```

## Project Structure

```
dagster_demo/
├── docker-compose.yml
├── README.md
├── .gitignore
├── endpoints/
│   ├── mysql-endpoint/
│   │   ├── Dockerfile.generator    # Separate data generator container
│   │   ├── init.sql               # MySQL schema initialization
│   │   └── data-generator.py      # Python script to generate data
│   ├── postgres-endpoint/
│   │   ├── Dockerfile.generator    # Separate data generator container
│   │   ├── init.sql               # PostgreSQL schema initialization
│   │   └── data-generator.py      # Python script to generate data
│   └── file-endpoint/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── file-generator.py      # Generates XML/KMZ/images
├── dagster/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dagster.yaml
│   └── dagster_etl/
│       ├── __init__.py           # Dagster definitions
│       ├── resources.py          # Database connection resources
│       ├── assets.py             # ETL assets for each endpoint type
│       ├── sensors.py            # Sensor monitoring ingest_control
│       └── jobs.py               # Job definitions
└── supabase/
    └── init.sql                  # Central DB schema + control table
```

## Key Features Demonstrated

✅ Sensor-based monitoring of control table
✅ Dynamic job creation based on configuration
✅ Multiple endpoint types (MySQL, PostgreSQL, Files)
✅ Incremental data loading with chunking
✅ Error handling for unstable endpoints
✅ Centralized configuration via database
✅ Real-time data generation
✅ Fully containerized architecture

## Next Steps

- Add authentication to Dagster UI
- Implement data quality checks
- Add alerting for failed ingestions
- Scale endpoints with multiple instances
- Add data transformation logic
- Implement data retention policies
