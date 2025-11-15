from dagster import asset, OpExecutionContext, AssetExecutionContext
from .resources import SupabaseResource, MySQLEndpointResource, PostgresEndpointResource
import os
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime


@asset
def ingest_mysql_data(
    context: AssetExecutionContext,
    supabase: SupabaseResource,
    mysql_endpoint: MySQLEndpointResource
) -> dict:
    """
    Ingest data from MySQL endpoint to Supabase with multi-chunk processing for backfill support
    """
    # Get endpoint configuration from context
    endpoint_name = context.run.tags.get('endpoint_name', 'Unknown')
    endpoint_host = context.run.tags.get('endpoint_host', 'mysql-endpoint')
    endpoint_port = int(context.run.tags.get('endpoint_port', '3306'))
    endpoint_db = context.run.tags.get('endpoint_db', 'sensors')
    chunk_size = int(context.run.tags.get('chunk_size', '50'))
    max_chunks_per_run = int(context.run.tags.get('max_chunks_per_run', '20'))

    context.log.info(f"Starting ingestion from MySQL endpoint: {endpoint_name}")
    context.log.info(f"Configuration: chunk_size={chunk_size}, max_chunks_per_run={max_chunks_per_run}")

    try:
        # Get last ingested ID from Supabase
        query = """
            SELECT MAX(source_id) as last_id
            FROM accelerometer_data
            WHERE endpoint_name = %s
        """
        result = supabase.execute_query(query, (endpoint_name,))
        last_id = result[0]['last_id'] if result and result[0]['last_id'] else 0
        starting_id = last_id

        context.log.info(f"Starting from ID: {last_id}")

        # Multi-chunk processing loop
        total_ingested = 0
        chunks_processed = 0
        columns = ['endpoint_name', 'timestamp', 'accel_x', 'accel_y', 'accel_z', 'source_id']

        for chunk_num in range(max_chunks_per_run):
            # Fetch next chunk
            measurements = mysql_endpoint.fetch_measurements(
                endpoint_host, endpoint_port, endpoint_db, chunk_size, last_id
            )

            if not measurements:
                context.log.info(f"No more measurements available after {chunks_processed} chunks")
                break

            # Prepare data for insertion
            values = [
                (endpoint_name, m['timestamp'], m['accel_x'], m['accel_y'], m['accel_z'], m['id'])
                for m in measurements
            ]

            # Insert into Supabase
            rows_inserted = supabase.insert_batch('accelerometer_data', columns, values)
            total_ingested += rows_inserted
            chunks_processed += 1

            # Update last_id for next iteration
            last_id = measurements[-1]['id']

            context.log.info(f"Chunk {chunks_processed}/{max_chunks_per_run}: Inserted {rows_inserted} records (total: {total_ingested}, last_id: {last_id})")

            # If we got fewer records than chunk_size, we've caught up
            if len(measurements) < chunk_size:
                context.log.info(f"Caught up! Received {len(measurements)} records (less than chunk_size={chunk_size})")
                break

        if total_ingested == 0:
            context.log.info("No new measurements to ingest")
        else:
            context.log.info(f"Successfully ingested {total_ingested} measurements across {chunks_processed} chunks from {endpoint_name}")
            context.log.info(f"ID range: {starting_id} -> {last_id} ({last_id - starting_id} IDs processed)")

        return {
            "ingested_count": total_ingested,
            "chunks_processed": chunks_processed,
            "endpoint": endpoint_name,
            "last_id": last_id,
            "starting_id": starting_id
        }

    except Exception as e:
        context.log.error(f"Error ingesting from MySQL endpoint {endpoint_name}: {str(e)}")
        raise


@asset
def ingest_postgres_data(
    context: AssetExecutionContext,
    supabase: SupabaseResource,
    postgres_endpoint: PostgresEndpointResource
) -> dict:
    """
    Ingest data from PostgreSQL endpoint to Supabase with multi-chunk processing for backfill support
    """
    # Get endpoint configuration from context
    endpoint_name = context.run.tags.get('endpoint_name', 'Unknown')
    endpoint_host = context.run.tags.get('endpoint_host', 'postgres-endpoint')
    endpoint_port = int(context.run.tags.get('endpoint_port', '5432'))
    endpoint_db = context.run.tags.get('endpoint_db', 'sensors')
    chunk_size = int(context.run.tags.get('chunk_size', '50'))
    max_chunks_per_run = int(context.run.tags.get('max_chunks_per_run', '20'))

    context.log.info(f"Starting ingestion from PostgreSQL endpoint: {endpoint_name}")
    context.log.info(f"Configuration: chunk_size={chunk_size}, max_chunks_per_run={max_chunks_per_run}")

    try:
        # Get last ingested ID from Supabase
        query = """
            SELECT MAX(source_id) as last_id
            FROM accel_mag_data
            WHERE endpoint_name = %s
        """
        result = supabase.execute_query(query, (endpoint_name,))
        last_id = result[0]['last_id'] if result and result[0]['last_id'] else 0
        starting_id = last_id

        context.log.info(f"Starting from ID: {last_id}")

        # Multi-chunk processing loop
        total_ingested = 0
        chunks_processed = 0
        columns = ['endpoint_name', 'timestamp', 'accel_x', 'accel_y', 'accel_z',
                   'mag_x', 'mag_y', 'mag_z', 'source_id']

        for chunk_num in range(max_chunks_per_run):
            # Fetch next chunk
            measurements = postgres_endpoint.fetch_measurements(
                endpoint_host, endpoint_port, endpoint_db, chunk_size, last_id
            )

            if not measurements:
                context.log.info(f"No more measurements available after {chunks_processed} chunks")
                break

            # Prepare data for insertion
            values = [
                (endpoint_name, m['timestamp'], m['accel_x'], m['accel_y'], m['accel_z'],
                 m['mag_x'], m['mag_y'], m['mag_z'], m['id'])
                for m in measurements
            ]

            # Insert into Supabase
            rows_inserted = supabase.insert_batch('accel_mag_data', columns, values)
            total_ingested += rows_inserted
            chunks_processed += 1

            # Update last_id for next iteration
            last_id = measurements[-1]['id']

            context.log.info(f"Chunk {chunks_processed}/{max_chunks_per_run}: Inserted {rows_inserted} records (total: {total_ingested}, last_id: {last_id})")

            # If we got fewer records than chunk_size, we've caught up
            if len(measurements) < chunk_size:
                context.log.info(f"Caught up! Received {len(measurements)} records (less than chunk_size={chunk_size})")
                break

        if total_ingested == 0:
            context.log.info("No new measurements to ingest")
        else:
            context.log.info(f"Successfully ingested {total_ingested} measurements across {chunks_processed} chunks from {endpoint_name}")
            context.log.info(f"ID range: {starting_id} -> {last_id} ({last_id - starting_id} IDs processed)")

        return {
            "ingested_count": total_ingested,
            "chunks_processed": chunks_processed,
            "endpoint": endpoint_name,
            "last_id": last_id,
            "starting_id": starting_id
        }

    except Exception as e:
        context.log.error(f"Error ingesting from PostgreSQL endpoint {endpoint_name}: {str(e)}")
        raise


@asset
def ingest_file_data(
    context: AssetExecutionContext,
    supabase: SupabaseResource
) -> dict:
    """
    Ingest file metadata from file endpoint to Supabase with batch processing for backfill support
    """
    endpoint_name = context.run.tags.get('endpoint_name', 'Unknown')
    max_folders_per_run = int(context.run.tags.get('max_chunks_per_run', '50'))  # For files, this is folders per run

    context.log.info(f"Starting file ingestion from endpoint: {endpoint_name}")
    context.log.info(f"Configuration: max_folders_per_run={max_folders_per_run}")

    try:
        data_dir = Path("/data")

        if not data_dir.exists():
            context.log.warning(f"Data directory {data_dir} does not exist")
            return {"ingested_count": 0, "endpoint": endpoint_name}

        # Get already ingested folders
        query = """
            SELECT folder_path
            FROM file_metadata
            WHERE endpoint_name = %s
        """
        result = supabase.execute_query(query, (endpoint_name,))
        ingested_folders = {r['folder_path'] for r in result}

        # Find new folders (excluding .git and other hidden folders)
        new_folders = [
            f for f in data_dir.iterdir()
            if f.is_dir() and not f.name.startswith('.') and str(f) not in ingested_folders
        ]

        if not new_folders:
            context.log.info("No new folders to ingest")
            return {"ingested_count": 0, "endpoint": endpoint_name}

        # Limit to max_folders_per_run to avoid overwhelming the system
        total_new_folders = len(new_folders)
        folders_to_process = new_folders[:max_folders_per_run]

        context.log.info(f"Found {total_new_folders} new folders, processing {len(folders_to_process)} in this run")

        # Prepare metadata for insertion
        columns = ['endpoint_name', 'folder_path', 'xml_file', 'kmz_file', 'image_count', 'created_at']
        values = []

        for folder in folders_to_process:
            xml_file = None
            kmz_file = None
            image_count = 0

            for file in folder.iterdir():
                if file.suffix == '.xml':
                    xml_file = file.name
                elif file.suffix == '.kmz':
                    kmz_file = file.name
                elif file.suffix in ['.jpg', '.jpeg', '.png']:
                    image_count += 1

            # Parse timestamp from folder name (format: YYYYMMDD_HHMMSS)
            try:
                created_at = datetime.strptime(folder.name, "%Y%m%d_%H%M%S")
            except:
                created_at = datetime.fromtimestamp(folder.stat().st_mtime)

            values.append((
                endpoint_name,
                str(folder),
                xml_file,
                kmz_file,
                image_count,
                created_at
            ))

        # Insert into Supabase
        rows_inserted = supabase.insert_batch('file_metadata', columns, values)

        remaining_folders = total_new_folders - len(folders_to_process)

        context.log.info(f"Successfully ingested {rows_inserted} folder metadata from {endpoint_name}")
        if remaining_folders > 0:
            context.log.info(f"Still {remaining_folders} folders remaining for next run")
        else:
            context.log.info("All folders processed - caught up!")

        return {
            "ingested_count": rows_inserted,
            "endpoint": endpoint_name,
            "total_new_folders": total_new_folders,
            "remaining_folders": remaining_folders
        }

    except Exception as e:
        context.log.error(f"Error ingesting files from endpoint {endpoint_name}: {str(e)}")
        raise
