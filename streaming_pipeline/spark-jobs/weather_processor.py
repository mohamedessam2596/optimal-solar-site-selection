"""
Spark Streaming Job: Process Egypt weather data from Kafka and write to TimescaleDB
 
This job:
1. Reads weather data from Kafka topic (3 partitions)
2. Parses and transforms the flattened data structure
3. Writes to TimescaleDB in micro-batches
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    lit, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType
)

# Configuration from environment variables
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'egypt_weather_raw')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'timescaledb')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'weather_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'weather_user')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'weather_pass')
CHECKPOINT_LOCATION = os.environ.get('CHECKPOINT_LOCATION', '/tmp/spark-checkpoint')

# JDBC URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Define schema for flattened weather data
weather_data_schema = StructType([
    StructField("coord_lon", DoubleType()),
    StructField("coord_lat", DoubleType()),
    StructField("weather_id", IntegerType()),
    StructField("weather_main", StringType()),
    StructField("weather_description", StringType()),
    StructField("weather_icon", StringType()),
    StructField("base", StringType()),
    StructField("main_temp", DoubleType()),
    StructField("main_feels_like", DoubleType()),
    StructField("main_temp_min", DoubleType()),
    StructField("main_temp_max", DoubleType()),
    StructField("main_pressure", IntegerType()),
    StructField("main_humidity", IntegerType()),
    StructField("main_sea_level", IntegerType()),
    StructField("main_grnd_level", IntegerType()),
    StructField("visibility", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_deg", IntegerType()),
    StructField("clouds_all", IntegerType()),
    StructField("dt", LongType()),
    StructField("sys_type", IntegerType()),
    StructField("sys_id", IntegerType()),
    StructField("sys_country", StringType()),
    StructField("sys_sunrise", LongType()),
    StructField("sys_sunset", LongType()),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType()),
    StructField("recorded_at", StringType())
])

# Define schema for incoming Kafka messages
kafka_schema = StructType([
    StructField("governorate", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("datetime", StringType(), False),
    StructField("partition", IntegerType(), False),
    StructField("data", weather_data_schema)
])


def create_spark_session():
    """Create and configure Spark session."""
    return (SparkSession.builder
            .appName("EgyptWeatherProcessor")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.7.1")
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate())


def process_weather_data(df):
    """Transform weather data to match database schema."""
    return (df
            .select(
                col("governorate"),
                col("partition").alias("kafka_partition"),
                to_timestamp(col("timestamp")).alias("timestamp"),
                
                # Coordinates
                col("data.coord_lon").alias("coord_lon"),
                col("data.coord_lat").alias("coord_lat"),
                
                # Weather
                col("data.weather_id").alias("weather_id"),
                col("data.weather_main").alias("weather_main"),
                col("data.weather_description").alias("weather_description"),
                col("data.weather_icon").alias("weather_icon"),
                
                # Base
                col("data.base").alias("base"),
                
                # Main metrics
                spark_round(col("data.main_temp"), 2).alias("main_temp"),
                spark_round(col("data.main_feels_like"), 2).alias("main_feels_like"),
                spark_round(col("data.main_temp_min"), 2).alias("main_temp_min"),
                spark_round(col("data.main_temp_max"), 2).alias("main_temp_max"),
                col("data.main_pressure").alias("main_pressure"),
                col("data.main_humidity").alias("main_humidity"),
                col("data.main_sea_level").alias("main_sea_level"),
                col("data.main_grnd_level").alias("main_grnd_level"),
                
                # Visibility
                col("data.visibility").alias("visibility"),
                
                # Wind
                spark_round(col("data.wind_speed"), 2).alias("wind_speed"),
                col("data.wind_deg").alias("wind_deg"),
                
                # Clouds
                col("data.clouds_all").alias("clouds_all"),
                
                # Timestamp from API
                col("data.dt").alias("dt"),
                
                # System info
                col("data.sys_type").alias("sys_type"),
                col("data.sys_id").alias("sys_id"),
                col("data.sys_country").alias("sys_country"),
                col("data.sys_sunrise").alias("sys_sunrise"),
                col("data.sys_sunset").alias("sys_sunset"),
                
                # Timezone
                col("data.timezone").alias("timezone"),
                
                # Location
                col("data.id").alias("location_id"),
                col("data.name").alias("location_name"),
                
                # Response code
                col("data.cod").alias("cod"),
                
                # Recording timestamp
                to_timestamp(col("data.recorded_at"), "yyyy-MM-dd HH:mm:ss").alias("recorded_at")
            )
            .withColumn("processed_at", current_timestamp())
    )


def write_to_postgres(batch_df, batch_id):
    """Write batch to PostgreSQL/TimescaleDB."""
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    if batch_df.count() > 0:
        # Show sample data for debugging
        print(f"Sample data from batch {batch_id}:")
        batch_df.select("governorate", "main_temp", "weather_main", "timestamp").show(5, truncate=False)
        
        (batch_df.write
         .format("jdbc")
         .option("url", JDBC_URL)
         .option("dbtable", "weather_data")
         .option("user", POSTGRES_USER)
         .option("password", POSTGRES_PASSWORD)
         .option("driver", "org.postgresql.Driver")
         .mode("append")
         .save())
        
        print(f"Batch {batch_id} written successfully to TimescaleDB")
    else:
        print(f"Batch {batch_id} is empty, skipping write")


def main():
    """Main streaming job."""
    print("Starting Egypt Weather Streaming Processor")
    print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"PostgreSQL: {JDBC_URL}")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Spark session created successfully")
    
    # Read from Kafka
    kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 100)
                .load())
    
    print("Connected to Kafka stream")
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select("data.*")
    
    # Process weather data
    processed_df = process_weather_data(parsed_df)
    
    # Write stream to PostgreSQL in micro-batches
    query = (processed_df
             .writeStream
             .foreachBatch(write_to_postgres)
             .outputMode("append")
             .option("checkpointLocation", CHECKPOINT_LOCATION)
             .trigger(processingTime="30 seconds")
             .start())
    
    print("Streaming query started. Processing data...")
    print("Press Ctrl+C to stop")
    
    # Wait for termination
    query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Error in streaming job: {e}", file=sys.stderr)
        sys.exit(1)
