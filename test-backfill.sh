#!/bin/bash

# Test script for Dagster backfill and catch-up detection
# This script demonstrates how the system handles historical data accumulated over time

set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Dagster Backfill & Catch-Up Test Script${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Check if we should clean up first
if [ "$1" = "--clean" ]; then
    echo -e "${YELLOW}Cleaning up existing containers and volumes...${NC}"
    docker-compose down -v
    echo -e "${GREEN}✓ Cleanup complete${NC}\n"
fi

echo -e "${YELLOW}Step 1: Starting endpoint services (without Dagster)${NC}"
echo -e "This allows data to accumulate before ingestion begins...\n"

docker-compose up -d supabase-db mysql-endpoint mysql-generator postgres-endpoint postgres-generator file-endpoint

echo -e "${GREEN}✓ Endpoint services started${NC}\n"

# Wait for databases to be ready
echo -e "${YELLOW}Waiting for databases to initialize...${NC}"
sleep 15
echo -e "${GREEN}✓ Databases ready${NC}\n"

# Ask user how long to accumulate data
echo -e "${YELLOW}Step 2: Accumulating historical data${NC}"
echo -e "How long should we let data accumulate? (in seconds)"
echo -e "  - 60 seconds  = ~600 MySQL records, ~300 Postgres records, ~6 file folders"
echo -e "  - 180 seconds = ~1800 MySQL records, ~900 Postgres records, ~18 file folders"
echo -e "  - 300 seconds = ~3000 MySQL records, ~1500 Postgres records, ~30 file folders"
read -p "Enter duration (default: 120): " DURATION
DURATION=${DURATION:-120}

echo -e "\n${BLUE}Accumulating data for ${DURATION} seconds...${NC}"
echo -e "Data generators are creating records. You can monitor progress:\n"
echo -e "  ${GREEN}docker-compose logs -f mysql-generator${NC}"
echo -e "  ${GREEN}docker-compose logs -f postgres-generator${NC}"
echo -e "  ${GREEN}docker-compose logs -f file-endpoint${NC}\n"

# Progress bar
for ((i=1; i<=DURATION; i++)); do
    echo -ne "${BLUE}Progress: [${i}/${DURATION}] $(($i*100/DURATION))%\r${NC}"
    sleep 1
done
echo -e "\n"

echo -e "${GREEN}✓ Data accumulation complete${NC}\n"

# Check accumulated data
echo -e "${YELLOW}Step 3: Checking accumulated data${NC}\n"

echo -e "${BLUE}MySQL Endpoint:${NC}"
MYSQL_COUNT=$(docker exec mysql-endpoint mysql -u sensoruser -psensorpass -D sensors -N -e "SELECT COUNT(*) FROM measurements" 2>/dev/null || echo "0")
echo -e "  Records: ${GREEN}${MYSQL_COUNT}${NC}"

echo -e "\n${BLUE}PostgreSQL Endpoint:${NC}"
POSTGRES_COUNT=$(docker exec postgres-endpoint psql -U sensoruser -d sensors -t -c "SELECT COUNT(*) FROM measurements" 2>/dev/null | xargs || echo "0")
echo -e "  Records: ${GREEN}${POSTGRES_COUNT}${NC}"

echo -e "\n${BLUE}File Endpoint:${NC}"
FILE_COUNT=$(docker exec file-endpoint find /data -maxdepth 1 -type d ! -path /data | wc -l)
echo -e "  Folders: ${GREEN}${FILE_COUNT}${NC}\n"

TOTAL=$((MYSQL_COUNT + POSTGRES_COUNT + FILE_COUNT))
if [ "$TOTAL" -eq 0 ]; then
    echo -e "${RED}⚠ Warning: No data accumulated. Data generators may not be running properly.${NC}"
    echo -e "${YELLOW}Check logs: docker-compose logs mysql-generator postgres-generator file-endpoint${NC}\n"
    read -p "Continue anyway? (y/n): " CONTINUE
    if [ "$CONTINUE" != "y" ]; then
        exit 1
    fi
fi

echo -e "${YELLOW}Step 4: Starting Dagster to begin backfill${NC}\n"
echo -e "${BLUE}Dagster will now process the accumulated historical data using multi-chunk processing.${NC}"
echo -e "${BLUE}Watch for 'Chunk X/Y' messages indicating backfill progress.${NC}\n"

read -p "Press Enter to start Dagster and begin backfill..."

docker-compose up -d dagster

echo -e "\n${GREEN}✓ Dagster started${NC}\n"

echo -e "${YELLOW}Step 5: Monitoring backfill progress${NC}\n"
echo -e "${BLUE}Waiting for Dagster to initialize...${NC}"
sleep 10

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Watch for these indicators of backfill:${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "  1. ${BLUE}Configuration: chunk_size=50, max_chunks_per_run=20${NC}"
echo -e "  2. ${BLUE}Chunk 1/20: Inserted 50 records...${NC}"
echo -e "  3. ${BLUE}Chunk N/20: Inserted X records (total: Y, last_id: Z)${NC}"
echo -e "  4. ${BLUE}Caught up! Received X records (less than chunk_size=50)${NC}"
echo -e "  5. ${BLUE}ID range: A -> B (N IDs processed)${NC}\n"

echo -e "${YELLOW}Following Dagster logs... (Ctrl+C to stop)${NC}\n"
echo -e "${GREEN}========================================${NC}\n"

docker-compose logs -f dagster

# This won't be reached unless user stops the logs
echo -e "\n${BLUE}To continue monitoring:${NC}"
echo -e "  ${GREEN}docker-compose logs -f dagster${NC}\n"

echo -e "${BLUE}To query ingested data:${NC}"
echo -e "  ${GREEN}docker exec -it supabase-db psql -U postgres -d postgres${NC}"
echo -e "  ${GREEN}SELECT endpoint_name, COUNT(*) FROM accelerometer_data GROUP BY endpoint_name;${NC}\n"

echo -e "${BLUE}To stop all services:${NC}"
echo -e "  ${GREEN}docker-compose down${NC}\n"

echo -e "${BLUE}To stop and remove all data:${NC}"
echo -e "  ${GREEN}docker-compose down -v${NC}\n"
