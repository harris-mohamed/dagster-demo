from dagster import define_asset_job, AssetSelection
from .assets import ingest_mysql_data, ingest_postgres_data, ingest_file_data

# Define the ETL job that can run any of the ingestion assets
etl_job = define_asset_job(
    name="etl_job",
    selection=AssetSelection.assets(ingest_mysql_data, ingest_postgres_data, ingest_file_data),
    description="ETL job for ingesting data from various endpoints to Supabase"
)
