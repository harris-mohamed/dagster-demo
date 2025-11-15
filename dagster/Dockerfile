FROM python:3.11-slim

WORKDIR /opt/dagster

# Install system dependencies
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Dagster code
COPY dagster.yaml .
COPY dagster_etl ./dagster_etl

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster

# Expose Dagster webserver port
EXPOSE 3000

# Start Dagster webserver and daemon
CMD dagster-daemon run & dagster dev -h 0.0.0.0 -p 3000 -m dagster_etl
