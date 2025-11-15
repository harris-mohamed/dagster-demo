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
    Ingest data from MySQL endpoint to Supabase
    """
    # Get endpoint configuration from context
    endpoint_name = context.run.tags.get('endpoint_name', 'Unknown')
    endpoint_host = context.run.tags.get('endpoint_host', 'mysql-endpoint')
    endpoint_port = int(context.run.tags.get('endpoint_port', '3306'))
    endpoint_db = context.run.tags.get('endpoint_db', 'sensors')
    chunk_size = int(context.run.tags.get('chunk_size', '50'))

    context.log.info(f"Starting ingestion from MySQL endpoint: {endpoint_name}")

    try:
        # Get last ingested ID from Supabase
        query = """
            SELECT MAX(source_id) as last_id
            FROM accelerometer_data
            WHERE endpoint_name = %s
        """
        result = supabase.execute_query(query, (endpoint_name,))
        last_id = result[0]['last_id'] if result and result[0]['last_id'] else 0

        context.log.info(f"Last ingested ID: {last_id}")

        # Fetch new measurements
        measurements = mysql_endpoint.fetch_measurements(
            endpoint_host, endpoint_port, endpoint_db, chunk_size, last_id
        )

        if not measurements:
            context.log.info("No new measurements to ingest")
            return {"ingested_count": 0, "endpoint": endpoint_name}

        # Prepare data for insertion
        columns = ['endpoint_name', 'timestamp', 'accel_x', 'accel_y', 'accel_z', 'source_id']
        values = [
            (endpoint_name, m['timestamp'], m['accel_x'], m['accel_y'], m['accel_z'], m['id'])
            for m in measurements
        ]

        # Insert into Supabase
        rows_inserted = supabase.insert_batch('accelerometer_data', columns, values)

        context.log.info(f"Successfully ingested {rows_inserted} measurements from {endpoint_name}")

        return {
            "ingested_count": rows_inserted,
            "endpoint": endpoint_name,
            "last_id": measurements[-1]['id'] if measurements else last_id
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
    Ingest data from PostgreSQL endpoint to Supabase
    """
    endpoint_name = context.run.tags.get('endpoint_name', 'Unknown')
    endpoint_host = context.run.tags.get('endpoint_host', 'postgres-endpoint')
    endpoint_port = int(context.run.tags.get('endpoint_port', '5432'))
    endpoint_db = context.run.tags.get('endpoint_db', 'sensors')
    chunk_size = int(context.run.tags.get('chunk_size', '50'))

    context.log.info(f"Starting ingestion from PostgreSQL endpoint: {endpoint_name}")

    try:
        # Get last ingested ID from Supabase
        query = """
            SELECT MAX(source_id) as last_id
            FROM accel_mag_data
            WHERE endpoint_name = %s
        """
        result = supabase.execute_query(query, (endpoint_name,))
        last_id = result[0]['last_id'] if result and result[0]['last_id'] else 0

        context.log.info(f"Last ingested ID: {last_id}")

        # Fetch new measurements
        measurements = postgres_endpoint.fetch_measurements(
            endpoint_host, endpoint_port, endpoint_db, chunk_size, last_id
        )

        if not measurements:
            context.log.info("No new measurements to ingest")
            return {"ingested_count": 0, "endpoint": endpoint_name}

        # Prepare data for insertion
        columns = ['endpoint_name', 'timestamp', 'accel_x', 'accel_y', 'accel_z',
                   'mag_x', 'mag_y', 'mag_z', 'source_id']
        values = [
            (endpoint_name, m['timestamp'], m['accel_x'], m['accel_y'], m['accel_z'],
             m['mag_x'], m['mag_y'], m['mag_z'], m['id'])
            for m in measurements
        ]

        # Insert into Supabase
        rows_inserted = supabase.insert_batch('accel_mag_data', columns, values)

        context.log.info(f"Successfully ingested {rows_inserted} measurements from {endpoint_name}")

        return {
            "ingested_count": rows_inserted,
            "endpoint": endpoint_name,
            "last_id": measurements[-1]['id'] if measurements else last_id
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
    Ingest file metadata from file endpoint to Supabase
    """
    endpoint_name = context.run.tags.get('endpoint_name', 'Unknown')

    context.log.info(f"Starting file ingestion from endpoint: {endpoint_name}")

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

        # Find new folders
        new_folders = [
            f for f in data_dir.iterdir()
            if f.is_dir() and str(f) not in ingested_folders
        ]

        if not new_folders:
            context.log.info("No new folders to ingest")
            return {"ingested_count": 0, "endpoint": endpoint_name}

        # Prepare metadata for insertion
        columns = ['endpoint_name', 'folder_path', 'xml_file', 'kmz_file', 'image_count', 'created_at']
        values = []

        for folder in new_folders:
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

        context.log.info(f"Successfully ingested {rows_inserted} folder metadata from {endpoint_name}")

        return {
            "ingested_count": rows_inserted,
            "endpoint": endpoint_name
        }

    except Exception as e:
        context.log.error(f"Error ingesting files from endpoint {endpoint_name}: {str(e)}")
        raise
