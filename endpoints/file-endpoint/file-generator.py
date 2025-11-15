#!/usr/bin/env python3
import os
import time
import random
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
from pathlib import Path
from PIL import Image
import xml.etree.ElementTree as ET
import zipfile

DATA_DIR = Path("/data")

def generate_xml_file(folder_path, timestamp):
    """Generate a sample XML file"""
    root = ET.Element("measurement")
    ET.SubElement(root, "timestamp").text = timestamp.isoformat()
    ET.SubElement(root, "latitude").text = str(round(random.uniform(-90, 90), 6))
    ET.SubElement(root, "longitude").text = str(round(random.uniform(-180, 180), 6))
    ET.SubElement(root, "altitude").text = str(round(random.uniform(0, 1000), 2))

    tree = ET.ElementTree(root)
    xml_path = folder_path / "measurement.xml"
    tree.write(xml_path, encoding='utf-8', xml_declaration=True)
    return xml_path.name

def generate_kmz_file(folder_path, timestamp):
    """Generate a sample KMZ file (zipped KML)"""
    kml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>Measurement {timestamp.isoformat()}</name>
    <Placemark>
      <name>Location</name>
      <Point>
        <coordinates>{random.uniform(-180, 180)},{random.uniform(-90, 90)},0</coordinates>
      </Point>
    </Placemark>
  </Document>
</kml>"""

    kmz_path = folder_path / "location.kmz"
    with zipfile.ZipFile(kmz_path, 'w', zipfile.ZIP_DEFLATED) as kmz:
        kmz.writestr("doc.kml", kml_content)

    return kmz_path.name

def generate_images(folder_path, count=3):
    """Generate sample images"""
    images_created = []
    for i in range(count):
        # Create a random colored image
        width, height = 640, 480
        img = Image.new('RGB', (width, height),
                       color=(random.randint(0, 255),
                             random.randint(0, 255),
                             random.randint(0, 255)))

        image_path = folder_path / f"image_{i+1}.jpg"
        img.save(image_path, "JPEG")
        images_created.append(image_path.name)

    return images_created

def generate_folder():
    """Generate a new folder with XML, KMZ, and images"""
    timestamp = datetime.now()
    folder_name = timestamp.strftime("%Y%m%d_%H%M%S")
    folder_path = DATA_DIR / folder_name
    folder_path.mkdir(parents=True, exist_ok=True)

    # Generate files
    xml_file = generate_xml_file(folder_path, timestamp)
    kmz_file = generate_kmz_file(folder_path, timestamp)
    images = generate_images(folder_path, count=random.randint(2, 5))

    print(f"[{datetime.now()}] Generated folder: {folder_name}")
    print(f"  - {xml_file}")
    print(f"  - {kmz_file}")
    print(f"  - {len(images)} images: {', '.join(images)}")

    return folder_path

def data_generator():
    """Generate folders every 10 seconds"""
    print("Starting file generator...")
    time.sleep(5)  # Wait a bit before starting

    while True:
        try:
            generate_folder()
            time.sleep(10)
        except Exception as e:
            print(f"Error generating folder: {e}")
            time.sleep(10)

class CustomHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DATA_DIR), **kwargs)

    def log_message(self, format, *args):
        # Suppress HTTP server logs
        pass

def run_http_server():
    """Run HTTP server to serve files"""
    server = HTTPServer(('0.0.0.0', 8000), CustomHTTPRequestHandler)
    print("HTTP server started on port 8000")
    server.serve_forever()

def main():
    # Start HTTP server in a separate thread
    server_thread = threading.Thread(target=run_http_server, daemon=True)
    server_thread.start()

    # Run data generator in main thread
    data_generator()

if __name__ == "__main__":
    main()
