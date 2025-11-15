#!/usr/bin/env python3
import mysql.connector
import time
import random
from datetime import datetime

def generate_data():
    """Generate random accelerometer data"""
    return {
        'accel_x': round(random.uniform(-10.0, 10.0), 6),
        'accel_y': round(random.uniform(-10.0, 10.0), 6),
        'accel_z': round(random.uniform(-10.0, 10.0), 6)
    }

def main():
    print("Starting MySQL data generator...")

    # Wait for MySQL to be fully ready
    time.sleep(5)

    # Connect to MySQL
    while True:
        try:
            conn = mysql.connector.connect(
                host='mysql-endpoint',
                user='sensoruser',
                password='sensorpass',
                database='sensors'
            )
            print("Connected to MySQL database")
            break
        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL: {e}")
            time.sleep(5)

    cursor = conn.cursor()

    # Generate data every 10 seconds
    while True:
        try:
            data = generate_data()
            query = """
                INSERT INTO measurements (timestamp, accel_x, accel_y, accel_z)
                VALUES (NOW(), %s, %s, %s)
            """
            cursor.execute(query, (data['accel_x'], data['accel_y'], data['accel_z']))
            conn.commit()

            print(f"[{datetime.now()}] Inserted: X={data['accel_x']}, Y={data['accel_y']}, Z={data['accel_z']}")

            time.sleep(10)
        except Exception as e:
            print(f"Error inserting data: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
