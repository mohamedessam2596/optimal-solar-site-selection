# OpenWeather API Configuration
OPENWEATHER_API_KEY=your_api_key_here
 
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=egypt_weather_raw
FETCH_INTERVAL_SECONDS=300

# TimescaleDB Configuration
POSTGRES_HOST=timescaledb
POSTGRES_PORT=5432
POSTGRES_DB=weather_db
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=weather_pass

# Airflow Configuration
AIRFLOW_UID=50000

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077

# Logging
LOG_LEVEL=INFO
