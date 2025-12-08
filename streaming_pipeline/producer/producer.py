#!/usr/bin/env python3
"""
Kafka Producer: Fetch current weather for 27 Egyptian governorates and push to Kafka topic.
Implements custom partitioning strategy based on geographic regions.

Configuration via environment variables:
  - OPENWEATHER_API_KEY (required)
  - KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092)
  - KAFKA_TOPIC (default: egypt_weather_raw)
  - FETCH_INTERVAL_SECONDS (default: 300)
  - TIMEOUT_SECONDS (default: 10)
"""

import os
import sys
import json
import time
import logging
from datetime import datetime
from pathlib import Path
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from partition_strategy import get_partition_for_governorate

# Setup logging
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'producer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('weather-producer')

# Configuration
BASE_DIR = Path(__file__).parent
GOV_FILE = BASE_DIR / 'governorates.json'

OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY', '206ca7733d71203484d60419b2cba30d')
if not OPENWEATHER_API_KEY:
    logger.error('OPENWEATHER_API_KEY environment variable is required.')
    sys.exit(1)

KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'egypt_weather_raw')
FETCH_INTERVAL = int(os.environ.get('FETCH_INTERVAL_SECONDS', '300'))
REQUEST_TIMEOUT = int(os.environ.get('TIMEOUT_SECONDS', '10'))

logger.info(f"Configuration loaded: Bootstrap={KAFKA_BOOTSTRAP}, Topic={KAFKA_TOPIC}, Interval={FETCH_INTERVAL}s")

# Load governorates data
try:
    with open(GOV_FILE, 'r', encoding='utf-8') as f:
        governorates = json.load(f)
    logger.info(f"Loaded {len(governorates)} governorates from {GOV_FILE}")
except Exception as e:
    logger.error(f"Failed to load governorates file: {e}")
    sys.exit(1)

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        retries=5,
        max_in_flight_requests_per_connection=5,
        acks='all',
        compression_type='gzip'
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    sys.exit(1)


def flatten_weather_data(data):
    """
    Flatten nested OpenWeather API response to match CSV structure.
    
    Returns dict with flattened fields matching:
    coord_lon, coord_lat, weather_id, weather_main, weather_description, etc.
    """
    try:
        weather_item = data.get('weather', [{}])[0] if data.get('weather') else {}
        
        flattened = {
            # Coordinates
            'coord_lon': data.get('coord', {}).get('lon'),
            'coord_lat': data.get('coord', {}).get('lat'),
            
            # Weather
            'weather_id': weather_item.get('id'),
            'weather_main': weather_item.get('main'),
            'weather_description': weather_item.get('description'),
            'weather_icon': weather_item.get('icon'),
            
            # Base
            'base': data.get('base'),
            
            # Main metrics (temperature in Kelvin from API, keep as is)
            'main_temp': data.get('main', {}).get('temp'),
            'main_feels_like': data.get('main', {}).get('feels_like'),
            'main_temp_min': data.get('main', {}).get('temp_min'),
            'main_temp_max': data.get('main', {}).get('temp_max'),
            'main_pressure': data.get('main', {}).get('pressure'),
            'main_humidity': data.get('main', {}).get('humidity'),
            'main_sea_level': data.get('main', {}).get('sea_level'),
            'main_grnd_level': data.get('main', {}).get('grnd_level'),
            
            # Visibility
            'visibility': data.get('visibility'),
            
            # Wind
            'wind_speed': data.get('wind', {}).get('speed'),
            'wind_deg': data.get('wind', {}).get('deg'),
            
            # Clouds
            'clouds_all': data.get('clouds', {}).get('all'),
            
            # Timestamp
            'dt': data.get('dt'),
            
            # System info
            'sys_type': data.get('sys', {}).get('type'),
            'sys_id': data.get('sys', {}).get('id'),
            'sys_country': data.get('sys', {}).get('country'),
            'sys_sunrise': data.get('sys', {}).get('sunrise'),
            'sys_sunset': data.get('sys', {}).get('sunset'),
            
            # Timezone
            'timezone': data.get('timezone'),
            
            # ID and name
            'id': data.get('id'),
            'name': data.get('name'),
            
            # Response code
            'cod': data.get('cod'),
            
            # Recording timestamp
            'recorded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return flattened
        
    except Exception as e:
        logger.error(f"Error flattening weather data: {e}")
        return None


def fetch_weather(lat, lon):
    """Fetch weather data from OpenWeather API."""
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.warning(f'Request timeout for coordinates {lat},{lon}')
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f'Failed to fetch weather for {lat},{lon}: {e}')
        return None


def produce_message(gov_name, raw_data):
    """Send weather data to Kafka with partition key."""
    # Flatten the data
    flattened_data = flatten_weather_data(raw_data)
    
    if not flattened_data:
        logger.error(f"Failed to flatten data for {gov_name}")
        return False
    
    partition = get_partition_for_governorate(gov_name)
    
    record = {
        "governorate": gov_name,
        "timestamp": int(time.time()),
        "datetime": datetime.utcnow().isoformat(),
        "partition": partition,
        "data": flattened_data
    }
    
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=gov_name,
            value=record,
            partition=partition
        )
        result = future.get(timeout=10)
        logger.info(
            f"Produced: {gov_name} -> Partition {result.partition}, "
            f"Offset {result.offset}, Temp: {flattened_data.get('main_temp', 'N/A')}°C"
        )
        return True
    except KafkaError as e:
        logger.error(f"Kafka error producing for {gov_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error sending message for {gov_name}: {e}")
        return False


def main_loop():
    """Main producer loop."""
    logger.info(f"Starting weather producer for {len(governorates)} governorates")
    logger.info(f"Fetching data every {FETCH_INTERVAL} seconds")
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        start_time = time.time()
        
        logger.info(f"--- Cycle {cycle_count} started at {datetime.now().isoformat()} ---")
        
        success_count = 0
        failure_count = 0
        
        for gov in governorates:
            name = gov.get('name')
            lat = gov.get('lat')
            lon = gov.get('lon')
            
            if not all([name, lat, lon]):
                logger.warning(f"Skipping incomplete governorate data: {gov}")
                failure_count += 1
                continue
            
            # Fetch weather data
            data = fetch_weather(lat, lon)
            
            if data:
                # Send to Kafka
                if produce_message(name, data):
                    success_count += 1
                else:
                    failure_count += 1
            else:
                failure_count += 1
            
            # Small delay to avoid API rate limiting
            time.sleep(0.2)
        
        # Calculate cycle metrics
        elapsed = time.time() - start_time
        sleep_for = max(0, FETCH_INTERVAL - elapsed)
        
        logger.info(
            f"--- Cycle {cycle_count} completed: "
            f"Success={success_count}, Failed={failure_count}, "
            f"Duration={elapsed:.2f}s, Sleep={sleep_for:.2f}s ---"
        )
        
        # Flush producer to ensure all messages are sent
        producer.flush()
        
        # Sleep until next cycle
        time.sleep(sleep_for)
#

if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer crashed: {e}", exc_info=True)
    finally:
        logger.info("Closing Kafka producer")
        producer.close()
        logger.info("Producer shutdown complete")
