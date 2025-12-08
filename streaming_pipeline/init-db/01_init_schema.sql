-- Initialize TimescaleDB extension and create schema for weather data
-- This script runs automatically when the database container starts
 
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create weather data table with flattened structure matching CSV format
CREATE TABLE IF NOT EXISTS weather_data (
    id BIGSERIAL,
    governorate VARCHAR(50) NOT NULL,
    kafka_partition INTEGER,
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Coordinates
    coord_lon DOUBLE PRECISION,
    coord_lat DOUBLE PRECISION,
    
    -- Weather information
    weather_id INTEGER,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    weather_icon VARCHAR(10),
    
    -- Base
    base VARCHAR(50),
    
    -- Main weather metrics (temperatures in Kelvin)
    main_temp DOUBLE PRECISION,
    main_feels_like DOUBLE PRECISION,
    main_temp_min DOUBLE PRECISION,
    main_temp_max DOUBLE PRECISION,
    main_pressure INTEGER,
    main_humidity INTEGER,
    main_sea_level INTEGER,
    main_grnd_level INTEGER,
    
    -- Visibility
    visibility INTEGER,
    
    -- Wind
    wind_speed DOUBLE PRECISION,
    wind_deg INTEGER,
    
    -- Clouds
    clouds_all INTEGER,
    
    -- API timestamp
    dt BIGINT,
    
    -- System information
    sys_type INTEGER,
    sys_id INTEGER,
    sys_country VARCHAR(2),
    sys_sunrise BIGINT,
    sys_sunset BIGINT,
    
    -- Timezone
    timezone INTEGER,
    
    -- Location ID and name
    location_id INTEGER,
    location_name VARCHAR(100),
    
    -- Response code
    cod INTEGER,
    
    -- Recording metadata
    recorded_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (governorate, timestamp)
);

-- Convert to hypertable (TimescaleDB)
SELECT create_hypertable('weather_data', 'timestamp', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_governorate ON weather_data (governorate, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_partition ON weather_data (kafka_partition);
CREATE INDEX IF NOT EXISTS idx_weather_main ON weather_data (weather_main);
CREATE INDEX IF NOT EXISTS idx_weather_location ON weather_data (location_name);
CREATE INDEX IF NOT EXISTS idx_weather_coords ON weather_data (coord_lat, coord_lon);

-- Add retention policy (keep data for 90 days)
SELECT add_retention_policy('weather_data', INTERVAL '90 days', if_not_exists => TRUE);

-- Create continuous aggregate for hourly statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS weather_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    governorate,
    COUNT(*) as reading_count,
    AVG(main_temp) as avg_temperature,
    MIN(main_temp_min) as min_temperature,
    MAX(main_temp_max) as max_temperature,
    AVG(main_humidity) as avg_humidity,
    AVG(wind_speed) as avg_wind_speed,
    AVG(main_pressure) as avg_pressure,
    AVG(visibility) as avg_visibility
FROM weather_data
GROUP BY bucket, governorate
WITH NO DATA;

-- Add refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('weather_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Create continuous aggregate for daily statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS weather_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', timestamp) AS bucket,
    governorate,
    COUNT(*) as reading_count,
    AVG(main_temp) as avg_temperature,
    MIN(main_temp_min) as min_temperature,
    MAX(main_temp_max) as max_temperature,
    AVG(main_humidity) as avg_humidity,
    AVG(wind_speed) as avg_wind_speed,
    AVG(main_pressure) as avg_pressure,
    MAX(clouds_all) as max_cloudiness
FROM weather_data
GROUP BY bucket, governorate
WITH NO DATA;

-- Add refresh policy for daily aggregate
SELECT add_continuous_aggregate_policy('weather_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create view for latest readings per governorate
CREATE OR REPLACE VIEW latest_weather AS
SELECT DISTINCT ON (governorate)
    governorate,
    timestamp,
    main_temp,
    main_feels_like,
    main_humidity,
    wind_speed,
    weather_main,
    weather_description,
    clouds_all,
    visibility,
    recorded_at
FROM weather_data
ORDER BY governorate, timestamp DESC;

-- Create view for partition statistics
CREATE OR REPLACE VIEW partition_stats AS
SELECT
    kafka_partition,
    COUNT(*) as total_records,
    COUNT(DISTINCT governorate) as governorate_count,
    MIN(timestamp) as first_reading,
    MAX(timestamp) as last_reading,
    AVG(main_temp) as avg_temperature
FROM weather_data
GROUP BY kafka_partition
ORDER BY kafka_partition;

-- Create view for temperature statistics by governorate
CREATE OR REPLACE VIEW governorate_temperature_stats AS
SELECT
    governorate,
    COUNT(*) as total_readings,
    AVG(main_temp) as avg_temp,
    MIN(main_temp_min) as all_time_min,
    MAX(main_temp_max) as all_time_max,
    STDDEV(main_temp) as temp_stddev,
    MIN(timestamp) as first_reading,
    MAX(timestamp) as last_reading
FROM weather_data
GROUP BY governorate
ORDER BY governorate;


-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO weather_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO weather_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO weather_user;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Weather database schema initialized successfully';
    RAISE NOTICE 'Created tables: weather_data';
    RAISE NOTICE 'Created materialized views: weather_hourly, weather_daily';
    RAISE NOTICE 'Created views: latest_weather, partition_stats, governorate_temperature_stats;
    RAISE NOTICE 'Created function: kelvin_to_celsius';
END $$;
