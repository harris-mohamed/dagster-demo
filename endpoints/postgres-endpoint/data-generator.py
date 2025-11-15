#!/usr/bin/env python3
import psycopg2
import time
import random
from datetime import datetime

def generate_data():
    """Generate random accelerometer + magnetometer data"""
    return {
        'accel_x': round(random.uniform(-10.0, 10.0), 6),
        'accel_y': round(random.uniform(-10.0, 10.0), 6),
        'accel_z': round(random.uniform(-10.0, 10.0), 6),
        'mag_x': round(random.uniform(-100.0, 100.0), 6),
        'mag_y': round(random.uniform(-100.0, 100.0), 6),
        'mag_z': round(random.uniform(-100.0, 100.0), 6)
    }

def main():
    print("Starting PostgreSQL data generator...")

    # Wait for PostgreSQL to be fully ready
    time.sleep(5)

    # Connect to PostgreSQL
    while True:
        try:
            conn = psycopg2.connect(
                host='postgres-endpoint',
                port=5432,
                user='sensoruser',
                password='sensorpass',
                database='sensors'
            )
            print("Connected to PostgreSQL database")
            break
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            time.sleep(5)

    cursor = conn.cursor()

    # Generate data every 10 seconds
    while True:
        try:
            data = generate_data()
            query = """
                INSERT INTO measurements (timestamp, accel_x, accel_y, accel_z, mag_x, mag_y, mag_z)
                VALUES (NOW(), %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                data['accel_x'], data['accel_y'], data['accel_z'],
                data['mag_x'], data['mag_y'], data['mag_z']
            ))
            conn.commit()

            print(f"[{datetime.now()}] Inserted: Accel=({data['accel_x']}, {data['accel_y']}, {data['accel_z']}), "
                  f"Mag=({data['mag_x']}, {data['mag_y']}, {data['mag_z']})")

            time.sleep(10)
        except Exception as e:
            print(f"Error inserting data: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
