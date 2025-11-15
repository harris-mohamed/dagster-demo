import os
import psycopg2
import mysql.connector
from dagster import ConfigurableResource
from typing import Dict, Any


class SupabaseResource(ConfigurableResource):
    """Resource for connecting to Supabase (PostgreSQL)"""

    host: str = "supabase-db"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    database: str = "postgres"

    def get_connection(self):
        """Get a database connection"""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def execute_query(self, query: str, params: tuple = None):
        """Execute a query and return results"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in results]
            else:
                conn.commit()
                return cursor.rowcount
        finally:
            conn.close()

    def insert_batch(self, table: str, columns: list, values: list):
        """Insert a batch of records"""
        if not values:
            return 0

        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            cursor.executemany(query, values)
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()


class MySQLEndpointResource(ConfigurableResource):
    """Resource for connecting to MySQL endpoints"""

    def get_connection(self, host: str, port: int, database: str, user: str = 'sensoruser', password: str = 'sensorpass'):
        """Get a MySQL connection"""
        return mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

    def fetch_measurements(self, host: str, port: int, database: str, chunk_size: int, last_id: int = 0):
        """Fetch measurements from MySQL endpoint"""
        conn = self.get_connection(host, port, database)
        try:
            cursor = conn.cursor(dictionary=True)
            query = """
                SELECT id, timestamp, accel_x, accel_y, accel_z
                FROM measurements
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """
            cursor.execute(query, (last_id, chunk_size))
            return cursor.fetchall()
        finally:
            conn.close()


class PostgresEndpointResource(ConfigurableResource):
    """Resource for connecting to PostgreSQL endpoints"""

    def get_connection(self, host: str, port: int, database: str, user: str = 'sensoruser', password: str = 'sensorpass'):
        """Get a PostgreSQL connection"""
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

    def fetch_measurements(self, host: str, port: int, database: str, chunk_size: int, last_id: int = 0):
        """Fetch measurements from PostgreSQL endpoint"""
        conn = self.get_connection(host, port, database)
        try:
            cursor = conn.cursor()
            query = """
                SELECT id, timestamp, accel_x, accel_y, accel_z, mag_x, mag_y, mag_z
                FROM measurements
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """
            cursor.execute(query, (last_id, chunk_size))
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()
