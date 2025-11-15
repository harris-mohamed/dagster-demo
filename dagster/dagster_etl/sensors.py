from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext, DefaultSensorStatus
from .resources import SupabaseResource
from .assets import ingest_mysql_data, ingest_postgres_data, ingest_file_data


@sensor(
    job_name="etl_job",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30
)
def endpoint_monitor_sensor(context: SensorEvaluationContext, supabase: SupabaseResource):
    """
    Sensor that monitors the ingest_control table and triggers ETL jobs for active endpoints
    """
    context.log.info("Checking ingest_control table for active endpoints...")

    try:
        # Query active endpoints
        query = """
            SELECT id, ip_address, port, name, chunk_size, max_chunks_per_run, endpoint_type, database_name
            FROM ingest_control
            WHERE active = true
            ORDER BY id
        """
        active_endpoints = supabase.execute_query(query)

        if not active_endpoints:
            return SkipReason("No active endpoints found")

        context.log.info(f"Found {len(active_endpoints)} active endpoint(s)")

        # Generate run requests for each active endpoint
        run_requests = []

        for endpoint in active_endpoints:
            endpoint_type = endpoint['endpoint_type']
            endpoint_name = endpoint['name']

            # Determine which asset to run based on endpoint type
            if endpoint_type == 'mysql':
                asset_selection = [ingest_mysql_data.key]
            elif endpoint_type == 'postgres':
                asset_selection = [ingest_postgres_data.key]
            elif endpoint_type == 'file':
                asset_selection = [ingest_file_data.key]
            else:
                context.log.warning(f"Unknown endpoint type: {endpoint_type}")
                continue

            # Create tags for the run
            tags = {
                "endpoint_name": endpoint_name,
                "endpoint_host": endpoint['ip_address'],
                "endpoint_port": str(endpoint['port']),
                "endpoint_type": endpoint_type,
                "chunk_size": str(endpoint['chunk_size']),
                "max_chunks_per_run": str(endpoint['max_chunks_per_run'])
            }

            # Add database name for database endpoints
            if endpoint.get('database_name'):
                tags["endpoint_db"] = endpoint['database_name']

            # Create run request
            import time
            run_key = f"{endpoint_type}_{endpoint_name}_{int(time.time())}"

            run_request = RunRequest(
                run_key=run_key,
                tags=tags,
                asset_selection=asset_selection
            )

            run_requests.append(run_request)
            context.log.info(f"Scheduling ETL for {endpoint_type} endpoint: {endpoint_name}")

        return run_requests

    except Exception as e:
        context.log.error(f"Error in endpoint_monitor_sensor: {str(e)}")
        return SkipReason(f"Error: {str(e)}")
