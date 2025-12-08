# Egypt Weather Streaming Pipeline 

A production-ready, end-to-end streaming data pipeline for real-time weather monitoring across all 27 Egyptian governorates with Kafka UI management via Conduktor Console.

## Architecture

```
┌──────────────┐
│ OpenWeather  │
│     API      │
└──────┬───────┘
       │
       v
┌──────────────┐      ┌─────────────────┐
│   Producer   │─────>│  Kafka Broker   │
│  (Python)    │      │  (3 Partitions) │
└──────────────┘      └────────┬────────┘
                               │
                      ┌────────┴────────┐
                      │                 │
                      v                 v
              ┌───────────────┐  ┌──────────────┐
              │ Spark Cluster │  │  Conduktor   │
              │ (1M + 3W)     │  │   Console    │
              └───────┬───────┘  │  (Kafka UI)  │
                      │          └──────────────┘
                      v
              ┌────────────────┐      ┌──────────┐
              │  TimescaleDB   │<─────│ Grafana  │
              │  (PostgreSQL)  │      └──────────┘
              └────────────────┘
                      ^
                      │
              ┌───────┴────────┐
              │    Airflow     │
              │ (Orchestrator) │
              └────────────────┘
```

## Features

- **Real-time Data Ingestion**: Fetches weather data every 5 minutes for 27 governorates
- **Flattened Data Structure**: CSV-compatible format with all weather fields
- **Intelligent Partitioning**: 3 Kafka partitions based on geographic regions
- **Distributed Processing**: Spark cluster with 1 master and 3 workers
- **Time-Series Storage**: TimescaleDB with continuous aggregates
- **Kafka Management UI**: Conduktor Console for visual Kafka monitoring
- **Orchestration**: Airflow for pipeline monitoring and scheduling
- **Visualization**: Grafana dashboards for real-time monitoring
- **Cross-Platform**: Works on Windows, Linux, and macOS

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka (KRaft) | 7.6.0 |
| Kafka UI | Conduktor Console | 1.24.1 |
| Processing | Apache Spark | 3.5.0 |
| Storage | TimescaleDB (PostgreSQL) | 16 |
| Orchestration | Apache Airflow | 2.8.1 |
| Visualization | Grafana | Latest |
| Language | Python | 3.11 |

## Prerequisites

- Docker Desktop 4.x+ (Windows/macOS) or Docker Engine 24.x+ (Linux)
- Docker Compose 2.x+
- 8GB+ RAM available for Docker
- OpenWeather API Key (free tier: https://openweathermap.org/api)

## Quick Start

### 1. Clone Repository

```bash
git clone <repository-url>
cd egypt-weather-streaming
```

### 2. Setup Environment

```bash
# Using Make (recommended)
make setup

# Or manually
mkdir -p producer spark-jobs airflow/dags airflow/logs airflow/plugins init-db conduktor
mkdir -p grafana/provisioning/datasources grafana/provisioning/dashboards grafana/dashboards
mkdir -p logs/producer logs/spark
cp .env.example .env
```

### 3. Configure API Key

Edit `.env` file and add your OpenWeather API key:

```bash
OPENWEATHER_API_KEY=your_actual_api_key_here
```

### 4. Start Pipeline

```bash
# Build and start all services
make build
make up

# Or using docker-compose directly
docker-compose build
docker-compose up -d
```

### 5. Verify Services

```bash
# Check service status
make status

# Comprehensive verification
make verify-all

# View logs
make logs
```

### 6. Access UIs

```bash
# Open all UIs
make conduktor-ui    # http://localhost:8088 (admin@conduktor.io/admin)
make airflow-ui      # http://localhost:8081 (admin/admin)
make spark-ui        # http://localhost:8080
make grafana-ui      # http://localhost:3000 (admin/admin)
```

## Data Structure

The pipeline processes flattened weather data matching this structure:

```csv
coord_lon,coord_lat,weather_id,weather_main,weather_description,weather_icon,
base,main_temp,main_feels_like,main_temp_min,main_temp_max,main_pressure,
main_humidity,main_sea_level,main_grnd_level,visibility,wind_speed,wind_deg,
clouds_all,dt,sys_type,sys_id,sys_country,sys_sunrise,sys_sunset,timezone,
id,name,cod,recorded_at
```

### Sample Data

```csv
31.2357,30.0444,800,Clear,clear sky,01d,stations,301.48,301.43,301.07,301.48,
1014,44,1014,1008,10000,6.17,360,0,1761746585,1,2514,EG,1761710794,1761750647,
10800,7922173,Al 'Atabah,200,2025-10-29 17:08:11
```

Note: Temperatures are in Kelvin (add 273.15 to Celsius). Use the built-in `kelvin_to_celsius()` function for conversions.

## Partition Strategy

27 governorates distributed across 3 Kafka partitions:

### Partition 0: Northern Region (9 governorates)
Alexandria, Beheira, Kafr El Sheikh, Dakahlia, Damietta, Port Said, Gharbia, Monufia, Qalyubia

### Partition 1: Central Region (9 governorates)
Cairo, Giza, Faiyum, Beni Suef, Minya, Asyut, Sohag, Qena, Helwan

### Partition 2: Southern & Border (9 governorates)
Luxor, Aswan, Red Sea, South Sinai, North Sinai, Ismailia, Suez, Matruh, New Valley

## Using Conduktor Console

Conduktor Console provides a modern, intuitive UI for Kafka management.

### Access

1. Navigate to http://localhost:8088
2. Login with: `admin@conduktor.io` / `admin`
3. Select "Egypt Weather Kafka Cluster"

### Features

**Topic Management:**
- View all topics and their configurations
- Monitor partition distribution
- Check retention settings
- View topic throughput metrics

**Message Browsing:**
- Browse messages in real-time
- Filter by partition
- Search message content
- View message headers and keys

**Consumer Monitoring:**
- View consumer groups
- Monitor consumer lag per partition
- Track consumption rates
- Identify slow consumers

**Cluster Health:**
- Monitor broker status
- View cluster metrics
- Check disk usage
- Monitor network throughput

### Common Tasks

**View Recent Messages:**
```
1. Go to Topics → egypt_weather_raw
2. Click "Data" tab
3. Select partition (0, 1, or 2)
4. Browse messages with formatted JSON
```

**Check Consumer Lag:**
```
1. Go to Consumer Groups
2. Select "weather-processing-group"
3. View lag per partition
4. Monitor consumption rate
```

**Verify Partitioning:**
```
1. Go to Topics → egypt_weather_raw
2. Click "Partitions" tab
3. Verify 3 partitions exist
4. Check message distribution
```

## Database Queries

The pipeline includes several convenient views and functions:

### Temperature Conversion

```sql
-- Built-in function
SELECT kelvin_to_celsius(301.48);  -- Returns 28.33

-- View with Celsius temperatures
SELECT * FROM weather_data_celsius 
ORDER BY timestamp DESC 
LIMIT 10;
```

### Latest Weather

```sql
-- Latest weather per governorate
SELECT * FROM latest_weather;

-- Latest temperature in Celsius
SELECT 
    governorate, 
    kelvin_to_celsius(main_temp) as temp_celsius,
    weather_main,
    timestamp
FROM weather_data 
ORDER BY timestamp DESC 
LIMIT 10;
```

### Statistics

```sql
-- Partition statistics
SELECT * FROM partition_stats;

-- Governorate temperature statistics
SELECT * FROM governorate_temperature_stats 
ORDER BY avg_temp DESC;

-- Hourly averages
SELECT * FROM weather_hourly 
WHERE governorate = 'Cairo' 
ORDER BY bucket DESC 
LIMIT 24;
```

### Using Make Commands

```bash
# View latest data in Celsius
make db-query-celsius

# View partition stats
make db-stats

# View governorate stats
make db-gov-stats

# View latest weather
make db-latest

# Custom query
make shell-db
```

## Common Operations

### View Logs

```bash
# All services
make logs

# Specific service
make logs-producer
make logs-spark
make logs-kafka
make logs-conduktor
make logs-airflow
```

### Kafka Operations

```bash
# List topics
make kafka-topics

# Describe topic with partitions
make kafka-describe

# Consume sample messages
make kafka-consume

# Or use Conduktor Console UI
make conduktor-ui
```

### Database Operations

```bash
# Quick query with Celsius conversion
make db-query

# View latest weather
make db-latest

# View statistics
make db-stats
make db-gov-stats

# Interactive SQL shell
make shell-db
```

### Service Management

```bash
# Check status
make status

# Restart all services
make restart

# Restart specific service
make restart-producer
make restart-conduktor

# Stop all
make down

# Clean everything (removes data)
make clean
```

## Monitoring

### Pipeline Health

```bash
# Comprehensive health check
make verify-all

# Check individual components
docker-compose ps
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092
docker exec timescaledb pg_isready -U weather_user
curl http://localhost:8088/health
```

### Conduktor Console Monitoring

1. Open http://localhost:8088
2. Dashboard shows:
   - Cluster health
   - Topic metrics
   - Consumer lag
   - Throughput graphs

### Key Metrics

**Producer:**
- Messages sent per cycle
- API response times
- Error rates

**Kafka:**
- Messages per partition
- Consumer lag
- Disk usage

**Spark:**
- Processing time per batch
- Record throughput
- Failed batches

**Database:**
- Insert rate
- Storage size
- Query performance

## Configuration

### Adjust Fetch Interval

Edit `.env`:
```bash
FETCH_INTERVAL_SECONDS=300  # Default: 5 minutes
```

### Scale Spark Workers

Edit `docker-compose.yml`:
```yaml
spark-worker-1:
  environment:
    SPARK_WORKER_MEMORY: 4G  # Increase memory
    SPARK_WORKER_CORES: 4    # Increase cores
```

### Change Data Retention

Edit `init-db/01_init_schema.sql`:
```sql
SELECT add_retention_policy('weather_data', INTERVAL '30 days');
```

### Adjust Kafka Partitions

Edit `docker-compose.yml` in kafka-init service:
```bash
kafka-topics --bootstrap-server kafka:29092 --create --topic egypt_weather_raw --partitions 6
```

Then update `producer/partition_strategy.py` accordingly.

## Troubleshooting

### Services Won't Start

```bash
# Check Docker resources
docker system df

# View detailed logs
docker-compose logs

# Restart specific service
docker-compose restart <service-name>

# Full restart
make restart
```

### Producer Not Sending Data

```bash
# Check producer logs
make logs-producer

# Verify API key
make test-api

# Restart producer
make restart-producer
```

### Conduktor Console Issues

```bash
# Check logs
make logs-conduktor

# Verify health
curl http://localhost:8088/health

# Restart
make restart-conduktor

# Access directly
docker exec -it conduktor-console /bin/sh
```

### Kafka Connection Issues

```bash
# Check Kafka health
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092

# List topics
make kafka-topics

# Use Conduktor Console for visual inspection
make conduktor-ui
```

### Database Issues

```bash
# Check connection
docker exec timescaledb pg_isready -U weather_user

# Interactive shell
make shell-db

# Verify data
SELECT COUNT(*) FROM weather_data;

# Check if temperatures are in Kelvin (273-320 range)
SELECT AVG(main_temp) FROM weather_data;
```

## Performance Tuning

### For Windows

- Allocate 8-16GB RAM to Docker Desktop
- Enable WSL2 backend
- Store project on WSL2 filesystem

### For Linux

- Adjust Docker daemon in `/etc/docker/daemon.json`
- Monitor with `docker stats`
- Use SSD for Docker volumes

### Optimization Tips

- Increase Spark worker memory for large batches
- Tune Kafka partition count for higher throughput
- Adjust micro-batch interval based on latency needs
- Enable TimescaleDB compression for old data
- Use appropriate retention policies

## File Changes from Original

1. **governorates.json** (renamed from governors.json)
   - Same content, renamed for clarity

2. **producer.py**
   - Added data flattening function
   - Matches CSV structure exactly
   - Temperature kept in Kelvin as per API

3. **init-db/01_init_schema.sql**
   - Flattened table structure
   - Added Celsius conversion function
   - Added weather_data_celsius view
   - More comprehensive indexes

4. **spark-jobs/weather_processor.py**
   - Updated schema to match flattened structure
   - Direct field mapping
   - Simplified transformations

5. **docker-compose.yml**
   - Added Conduktor Console service
   - Port 8088 for Conduktor UI

6. **conduktor/console-config.yml**
   - New file for Conduktor configuration
   - Connects to Kafka cluster

## Security Considerations

This is a development setup. For production:

1. Change default passwords in `.env`
2. Enable Kafka authentication (SASL/SSL)
3. Use PostgreSQL SSL connections
4. Implement Airflow RBAC
5. Secure Grafana with HTTPS
6. Use Docker secrets for credentials
7. Network segmentation
8. Enable audit logging

## License

MIT License - Free to use and modify

## Support

For issues and questions:
- Check logs: `make logs`
- Use Conduktor Console for Kafka issues
- Review database views for data issues
- Open an issue in the repository

## Acknowledgments

- OpenWeather API for weather data
- Apache Software Foundation for Kafka, Spark, Airflow
- Timescale Inc. for TimescaleDB
- Grafana Labs for Grafana
- Conduktor for Conduktor Console
