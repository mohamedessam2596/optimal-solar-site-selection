""" 
Airflow DAG: Egypt Weather Streaming Pipeline Orchestration

This DAG orchestrates the entire weather streaming pipeline:
1. Monitors Kafka topics health
2. Submits Spark streaming job
3. Monitors data quality
4. Sends alerts on failures
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

# Configuration
DEFAULT_ARGS = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

DAG_ID = 'egypt_weather_streaming_pipeline'
SCHEDULE_INTERVAL = '@hourly'

logger = logging.getLogger(__name__)


def check_kafka_health(**context):
    """Check if Kafka broker is healthy and topics exist."""
    from kafka import KafkaAdminClient, KafkaConsumer
    from kafka.errors import KafkaError
    
    bootstrap_servers = 'kafka:29092'
    topic_name = 'egypt_weather_raw'
    
    try:
        # Check broker connectivity
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='airflow-health-check'
        )
        
        # List topics
        topics = admin_client.list_topics()
        logger.info(f"Available topics: {topics}")
        
        if topic_name not in topics:
            raise ValueError(f"Required topic '{topic_name}' not found")
        
        # Check topic has partitions
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id='airflow-check'
        )
        partitions = consumer.partitions_for_topic(topic_name)
        
        if not partitions or len(partitions) != 3:
            raise ValueError(f"Expected 3 partitions, found {len(partitions) if partitions else 0}")
        
        logger.info(f"Kafka health check passed: {len(partitions)} partitions found")
        admin_client.close()
        consumer.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        raise


def check_producer_running(**context):
    """Check if weather producer is running and producing messages."""
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    import time
    
    bootstrap_servers = 'kafka:29092'
    topic_name = 'egypt_weather_raw'
    
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id='airflow-producer-check',
            auto_offset_reset='latest',
            consumer_timeout_ms=30000
        )
        
        # Check for recent messages
        message_count = 0
        start_time = time.time()
        
        for message in consumer:
            message_count += 1
            if message_count >= 3:  # Check for at least 3 messages
                break
            if time.time() - start_time > 30:  # Timeout after 30 seconds
                break
        
        consumer.close()
        
        if message_count == 0:
            logger.warning("No messages found in Kafka topic - producer may not be running")
            return False
        
        logger.info(f"Producer check passed: {message_count} messages found")
        return True
        
    except Exception as e:
        logger.error(f"Producer check failed: {e}")
        return False


def check_database_connection(**context):
    """Verify TimescaleDB connection and table existence."""
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host='timescaledb',
            port=5432,
            database='weather_db',
            user='weather_user',
            password='weather_pass'
        )
        
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'weather_data'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            raise ValueError("weather_data table does not exist")
        
        # Check recent data
        cursor.execute("""
            SELECT COUNT(*), MAX(timestamp) 
            FROM weather_data 
            WHERE timestamp > NOW() - INTERVAL '1 hour';
        """)
        
        count, latest = cursor.fetchone()
        logger.info(f"Database check passed: {count} records in last hour, latest: {latest}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        raise


def check_data_quality(**context):
    """Perform data quality checks on recent weather data."""
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host='timescaledb',
            port=5432,
            database='weather_db',
            user='weather_user',
            password='weather_pass'
        )
        
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT governorate) AS gov_count,
                COUNT(*) AS total_records,
                AVG(main_temp) AS avg_temp,
                MIN(main_temp) AS min_temp,
                MAX(main_temp) AS max_temp
            FROM weather_data
            WHERE timestamp > NOW() - INTERVAL '6 hours';
        """)
        
        gov_count, total_records, avg_temp, min_temp, max_temp = cursor.fetchone()
        
        logger.info(f"Governorates count: {gov_count}")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Avg temperature: {avg_temp if avg_temp is not None else 'NULL'}")
        logger.info(f"Min temperature: {min_temp if min_temp is not None else 'NULL'}")
        logger.info(f"Max temperature: {max_temp if max_temp is not None else 'NULL'}")
        
        issues = []

        # Coverage check
        if gov_count < 20:
            issues.append(f"Low governorate coverage: {gov_count}/27")

        # Record count check
        if total_records < 100:
            issues.append(f"Low record count: {total_records}")

        # Temperature sanity check
        if avg_temp is not None and (avg_temp < -10 or avg_temp > 60):
            issues.append(f"Suspicious avg_temp: {avg_temp}")

        cursor.close()
        conn.close()

        if issues:
            logger.warning(f"Data quality issues: {issues}")
            context['ti'].xcom_push(key='quality_issues', value=issues)

        return True

    except Exception as e:
        logger.error(f"Data quality check failed: {e}")
        raise



def log_pipeline_metrics(**context):
    """Log comprehensive pipeline metrics safely."""
    import psycopg2
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    metrics = {
        "database": {},
        "kafka": {}
    }

    try:
        # ---------------------------
        # Database Metrics Collection
        # ---------------------------
        conn = psycopg2.connect(
            host='timescaledb',
            port=5432,
            database='weather_db',
            user='weather_user',
            password='weather_pass'
        )
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                COUNT(*) AS total_records,
                COUNT(DISTINCT governorate) AS unique_governorates,
                MIN(timestamp) AS first_record,
                MAX(timestamp) AS last_record
            FROM weather_data;
        """)

        row = cursor.fetchone()

        metrics["database"] = {
            "total_records": row[0],
            "unique_governorates": row[1],
            "first_record": str(row[2]) if row[2] else None,
            "last_record": str(row[3]) if row[3] else None
        }

        cursor.close()
        conn.close()

        # ---------------------------
        # Kafka Metrics Collection
        # ---------------------------
        try:
            consumer = KafkaConsumer(
                bootstrap_servers='kafka:29092',
                group_id='airflow-metrics',
                consumer_timeout_ms=5000
            )

            partitions = consumer.partitions_for_topic('egypt_weather_raw')

            metrics["kafka"] = {
                "topic": "egypt_weather_raw",
                "partitions": len(partitions) if partitions else 0
            }

            consumer.close()

        except KafkaError as ke:
            logger.warning(f"Kafka metrics unavailable: {ke}")
            metrics["kafka"] = {
                "topic": "egypt_weather_raw",
                "partitions": 0,
                "error": str(ke)
            }

        # ---------------------------
        # Log + XCom Push
        # ---------------------------
        logger.info(f"Pipeline metrics collected successfully: {metrics}")
        context['ti'].xcom_push(key='pipeline_metrics', value=metrics)

        return True

    except Exception as e:
        logger.error(f"Failed to collect metrics: {e}")
        # Return True to avoid Airflow retries (optional choice)
        context['ti'].xcom_push(key='pipeline_metrics', value={"error": str(e)})
        return True


# Define the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Egypt Weather Streaming Pipeline Orchestration',
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=['weather', 'streaming', 'egypt'],
) as dag:
    
    # Start task
    start = DummyOperator(task_id='start')
    
    # Health checks
    kafka_health = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_health,
        provide_context=True
    )
    
    producer_check = PythonSensor(
        task_id='check_producer_running',
        python_callable=check_producer_running,
        poke_interval=30,
        timeout=300,
        mode='poke'
    )
    
    db_health = PythonOperator(
        task_id='check_database_connection',
        python_callable=check_database_connection,
        provide_context=True
    )
    
    # Data quality check
    #quality_check = PythonOperator(
     #   task_id='check_data_quality',
      #  python_callable=check_data_quality,
       # provide_context=True
    #)
    
    # Collect metrics
    #collect_metrics = PythonOperator(
     #   task_id='log_pipeline_metrics',
      #  python_callable=log_pipeline_metrics,
       # provide_context=True
    #)
    
    # End task
    end = DummyOperator(task_id='end')
    
    # Define task dependencies
   # start >> [kafka_health, db_health]
    #kafka_health >> producer_check
    #[producer_check, db_health] >> #quality_check
    #quality_check >> collect_metrics
    #collect_metrics >> end

    start >> kafka_health >> producer_check >> db_health  >> end
