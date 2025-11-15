from dagster import Definitions
from .assets import ingest_mysql_data, ingest_postgres_data, ingest_file_data
from .sensors import endpoint_monitor_sensor
from .jobs import etl_job
from .resources import SupabaseResource, MySQLEndpointResource, PostgresEndpointResource
import os

# Define all resources
resources = {
    "supabase": SupabaseResource(
        host=os.getenv("SUPABASE_HOST", "supabase-db"),
        port=int(os.getenv("SUPABASE_PORT", "5432")),
        user=os.getenv("SUPABASE_USER", "postgres"),
        password=os.getenv("SUPABASE_PASSWORD", "postgres"),
        database=os.getenv("SUPABASE_DB", "postgres")
    ),
    "mysql_endpoint": MySQLEndpointResource(),
    "postgres_endpoint": PostgresEndpointResource()
}

# Define the Dagster repository
defs = Definitions(
    assets=[ingest_mysql_data, ingest_postgres_data, ingest_file_data],
    sensors=[endpoint_monitor_sensor],
    jobs=[etl_job],
    resources=resources
)
