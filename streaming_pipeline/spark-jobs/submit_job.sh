#!/bin/bash
 
# Spark Job Submission Script for Weather Processing
# Works on both Linux and Windows (Git Bash/WSL)

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SPARK_MASTER=${SPARK_MASTER_URL:-"spark://spark-master:7077"}
APP_NAME="EgyptWeatherProcessor"
DRIVER_MEMORY="1g"
EXECUTOR_MEMORY="2g"
EXECUTOR_CORES="2"
NUM_EXECUTORS="3"

# Get script directory (works on Linux and Windows)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_FILE="${SCRIPT_DIR}/weather_processor.py"

# Check if job file exists
if [ ! -f "$JOB_FILE" ]; then
    echo -e "${RED}Error: Job file not found at ${JOB_FILE}${NC}"
    exit 1
fi

echo -e "${GREEN}=== Spark Job Submission ===${NC}"
echo "Job File: $JOB_FILE"
echo "Spark Master: $SPARK_MASTER"
echo "Application: $APP_NAME"
echo ""

# Submit the job
echo -e "${YELLOW}Submitting job to Spark cluster...${NC}"

spark-submit \
    --master "$SPARK_MASTER" \
    --name "$APP_NAME" \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --num-executors "$NUM_EXECUTORS" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoint \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.dynamicAllocation.enabled=false \
    "$JOB_FILE"

echo -e "${GREEN}Job submission completed${NC}"
