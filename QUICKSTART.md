# Quick Start Guide

## 1. Start the Demo

```bash
docker-compose up --build
```

(Run from the `dagster-demo` directory)

Wait for all containers to start (about 30-60 seconds). You'll see output from:
- `supabase-db` - Central database
- `mysql-endpoint` - MySQL database
- `mysql-generator` - Generating accelerometer data
- `postgres-endpoint` - PostgreSQL database
- `postgres-generator` - Generating accel+mag data
- `file-endpoint` - Generating XML/KMZ/images
- `dagster` - Orchestration platform

## 2. Access Dagster UI

Open your browser to: **http://localhost:3000**

### What You'll See:

1. **Overview Page**: Shows all assets and sensors
2. **Sensors Tab**: Click to see `endpoint_monitor_sensor` running
3. **Runs Tab**: Watch automatic ETL runs being triggered every 30 seconds

## 3. Verify Data Flow

### Watch the Sensor Work

1. Go to **Sensors** in the left sidebar
2. Click on `endpoint_monitor_sensor`
3. You'll see it evaluating every 30 seconds
4. Each evaluation triggers 3 runs (one for each endpoint)

### View Runs

1. Go to **Runs** in the left sidebar
2. You'll see runs tagged with endpoint information:
   - `endpoint_type: mysql`
   - `endpoint_type: postgres`
   - `endpoint_type: file`

### Check Asset Materializations

1. Go to **Assets** in the left sidebar
2. Click on any asset:
   - `ingest_mysql_data`
   - `ingest_postgres_data`
   - `ingest_file_data`
3. View materialization history and metadata

## 4. Query the Data

Connect to Supabase to see ingested data:

```bash
docker exec -it supabase-db psql -U postgres -d postgres
```

Then run queries:

```sql
-- Check control table
SELECT * FROM ingest_control;

-- Count ingested records by endpoint
SELECT endpoint_name, COUNT(*) FROM accelerometer_data GROUP BY endpoint_name;
SELECT endpoint_name, COUNT(*) FROM accel_mag_data GROUP BY endpoint_name;
SELECT endpoint_name, COUNT(*) FROM file_metadata GROUP BY endpoint_name;

-- View recent data
SELECT * FROM accelerometer_data ORDER BY ingested_at DESC LIMIT 10;
SELECT * FROM accel_mag_data ORDER BY ingested_at DESC LIMIT 10;
SELECT * FROM file_metadata ORDER BY ingested_at DESC LIMIT 10;

-- Exit
\q
```

## 5. View Logs

Open separate terminal windows to monitor each component:

```bash
# Dagster orchestration logs
docker-compose logs -f dagster

# MySQL data generation
docker-compose logs -f mysql-generator

# PostgreSQL data generation
docker-compose logs -f postgres-generator

# File generation
docker-compose logs -f file-endpoint
```

## 6. Control the ETL

### Pause an Endpoint

```bash
docker exec -it supabase-db psql -U postgres -d postgres
```

```sql
-- Disable MySQL endpoint
UPDATE ingest_control SET active = false WHERE endpoint_type = 'mysql';

-- The sensor will stop triggering runs for this endpoint
```

### Re-enable

```sql
UPDATE ingest_control SET active = true WHERE endpoint_type = 'mysql';
```

### Adjust Chunk Size

```sql
-- Ingest more records per run
UPDATE ingest_control SET chunk_size = 100 WHERE name = 'MySQL Accelerometer Sensor';
```

## 7. Stop the Demo

```bash
# Stop all containers
docker-compose down

# Stop and remove all data
docker-compose down -v
```

## Expected Behavior

- **Every 10 seconds**: Endpoints generate new data
- **Every 30 seconds**: Dagster sensor checks `ingest_control` table
- **Immediately after sensor**: ETL jobs run for active endpoints
- **Data flows**: Endpoints → Dagster ETL → Supabase

## Troubleshooting

### Dagster UI not loading?
- Wait 30-60 seconds for all services to start
- Check logs: `docker-compose logs dagster`
- Verify port 3000 is not in use: `lsof -i :3000`

### No data being ingested?
- Check sensor is running in Dagster UI
- Verify generators are running: `docker-compose ps`
- Check generator logs for errors
- Verify endpoints are marked active in `ingest_control`

### Containers failing to start?
- Check for port conflicts (3000, 3306, 5432, 5433, 8000)
- Ensure Docker has enough resources (4GB+ RAM recommended)
- View specific container logs: `docker-compose logs <service-name>`

## 8. Testing Backfill & Catch-Up Detection

The quickstart above demonstrates real-time incremental ingestion. To test the **multi-chunk backfill** feature for endpoints with historical data, use the automated test script:

```bash
./test-backfill.sh
```

### What This Script Does

1. **Starts only endpoint services** (databases + generators) without Dagster
2. **Accumulates data** for a configurable duration (default: 2 minutes)
3. **Shows accumulated counts** (MySQL records, Postgres records, file folders)
4. **Starts Dagster** to begin processing the backlog
5. **Monitors logs** showing multi-chunk processing in action

### What You'll Observe

When Dagster starts processing the accumulated data, you'll see:

```
Configuration: chunk_size=50, max_chunks_per_run=20
Starting from ID: 0
Chunk 1/20: Inserted 50 records (total: 50, last_id: 50)
Chunk 2/20: Inserted 50 records (total: 100, last_id: 100)
Chunk 3/20: Inserted 50 records (total: 150, last_id: 150)
...
Chunk 18/20: Inserted 50 records (total: 900, last_id: 900)
Chunk 19/20: Inserted 42 records (total: 942, last_id: 942)
Caught up! Received 42 records (less than chunk_size=50)
Successfully ingested 942 measurements across 19 chunks
ID range: 0 -> 942 (942 IDs processed)
```

This demonstrates:
- **Multi-chunk processing**: Processing 19 chunks in a single run instead of 19 separate runs
- **Catch-up detection**: Automatically detects when caught up (received < chunk_size)
- **Progress tracking**: Shows ID ranges and total records processed

### Script Options

```bash
# Clean up before starting (removes existing data)
./test-backfill.sh --clean

# Default run (accumulates data for 2 minutes)
./test-backfill.sh

# Custom duration can be entered interactively when prompted
```

### Manual Testing Alternative

If you prefer manual control:

```bash
# 1. Start endpoints only
docker-compose up -d supabase-db mysql-endpoint mysql-generator postgres-endpoint postgres-generator file-endpoint

# 2. Wait 3-5 minutes for data to accumulate

# 3. Check accumulated data
docker exec mysql-endpoint mysql -u sensoruser -psensorpass -D sensors -e "SELECT COUNT(*) FROM measurements"

# 4. Start Dagster
docker-compose up -d dagster

# 5. Watch the backfill
docker-compose logs -f dagster
```

## Next Steps

1. Explore the code in `dagster/dagster_etl/`
2. Add a new endpoint to `ingest_control`
3. Modify `chunk_size` and `max_chunks_per_run` to see different ingestion patterns
4. Test with longer accumulation periods (10-15 minutes) to see multiple sensor cycles
5. Check the Dagster sensor evaluation logs
6. Review the asset metadata after materializations

Enjoy exploring Dagster as an ETL sensor platform!
