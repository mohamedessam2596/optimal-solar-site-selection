from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from docker.types import Mount
import os
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=2),
}

def upload_new_files_to_azure():
    """Check new_data/ directory and upload any new CSV files to Azure Blob Storage"""
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = os.getenv('AZURE_CONTAINER_NAME')
    
    new_data_path = Path('/opt/airflow/new_data')
    
    if not new_data_path.exists():
        print("⚠ new_data/ directory does not exist. Creating it...")
        new_data_path.mkdir(parents=True, exist_ok=True)
        print("✓ new_data/ directory created")
        return []
    
    csv_files = list(new_data_path.glob('*.csv'))
    
    if not csv_files:
        print("ℹ No CSV files found in new_data/ directory")
        print("ℹ This is expected for initial run with only historical data in Azure")
        return []
    
    print(f"Found {len(csv_files)} CSV file(s) in new_data/ directory")
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    existing_blobs = set()
    try:
        blobs = container_client.list_blobs(name_starts_with='bronze/')
        for blob in blobs:
            existing_blobs.add(blob.name.split('/')[-1])
        print(f"Found {len(existing_blobs)} existing files in Azure bronze/ directory")
    except Exception as e:
        print(f"⚠ Warning: Could not list existing blobs: {str(e)}")
    
    uploaded_files = []
    skipped_files = []
    
    for csv_file in csv_files:
        filename = csv_file.name
        blob_name = f"bronze/{filename}"
        
        if filename in existing_blobs:
            print(f"⊘ Skipping {filename} (already exists in Azure)")
            skipped_files.append(filename)
            continue
        
        try:
            blob_client = container_client.get_blob_client(blob_name)
            
            with open(csv_file, 'rb') as data:
                blob_client.upload_blob(data, overwrite=False)
            
            print(f"✓ Uploaded: {filename} → {blob_name}")
            uploaded_files.append(filename)
            
        except Exception as e:
            print(f"✗ Error uploading {filename}: {str(e)}")
    
    print("\n" + "=" * 60)
    print(f"Upload Summary:")
    print(f"  - Files found: {len(csv_files)}")
    print(f"  - Uploaded: {len(uploaded_files)}")
    print(f"  - Skipped: {len(skipped_files)}")
    print("=" * 60)
    
    return uploaded_files

def check_files_in_azure():
    """Check and list all files in Azure blob storage bronze/ directory"""
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = os.getenv('AZURE_CONTAINER_NAME')
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    blobs = container_client.list_blobs(name_starts_with='bronze/')
    
    files_found = []
    file_types = {'air_pollution': 0, 'solar_energy': 0, 'weather': 0}
    
    for blob in blobs:
        if blob.name.endswith('.csv'):
            files_found.append(blob.name)
            if 'air_pollution' in blob.name.lower():
                file_types['air_pollution'] += 1
            elif 'solar_energy' in blob.name.lower():
                file_types['solar_energy'] += 1
            elif 'weather' in blob.name.lower():
                file_types['weather'] += 1
    
    print(f"\n{'=' * 60}")
    print(f"Azure Bronze Directory Inventory:")
    print(f"{'=' * 60}")
    print(f"Total CSV files: {len(files_found)}")
    print(f"  - Air Pollution: {file_types['air_pollution']} files")
    print(f"  - Solar Energy: {file_types['solar_energy']} files")
    print(f"  - Weather: {file_types['weather']} files")
    print(f"{'=' * 60}\n")
    
    for file in files_found:
        print(f"  - {file}")
    
    if len(files_found) == 0:
        raise ValueError("No CSV files found in Azure bronze/ directory. Please upload historical data first.")
    
    return files_found

def load_data_from_azure():
    """Load data from Azure Blob Storage to Snowflake using COPY INTO"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    print("\n" + "=" * 60)
    print("LOADING DATA FROM AZURE TO SNOWFLAKE")
    print("=" * 60)
    
    results = {}
    
    # Load AIR_POLLUTION data
    print("\n1. Loading AIR_POLLUTION data...")
    air_pollution_copy = """
    COPY INTO BRONZE.AIR_POLLUTION (
        CITY, DATETIME, AQI, CO, NO, NO2, O3, SO2, PM2_5, PM10, NH3, LOAD_TIMESTAMP
    )
    FROM (
        SELECT 
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            CURRENT_TIMESTAMP()
        FROM @BRONZE.AZURE_BRONZE_STAGE
    )
    PATTERN='.*air_pollution.*\\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FORMAT')
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;
    """
    
    try:
        result = hook.run(air_pollution_copy)
        results['air_pollution'] = result
        print(f"✓ AIR_POLLUTION loaded successfully")
    except Exception as e:
        print(f"⚠ AIR_POLLUTION load completed with warnings: {str(e)}")
        results['air_pollution'] = str(e)
    
    # Load SOLAR_ENERGY data
    print("\n2. Loading SOLAR_ENERGY data...")
    solar_energy_copy = """
    COPY INTO BRONZE.SOLAR_ENERGY (
        YEAR, MO, DY, HR, ALLSKY_SFC_SW_DWN, CLRSKY_SFC_SW_DWN, 
        ALLSKY_SFC_SW_DNI, ALLSKY_SFC_SW_DIFF, ALLSKY_SFC_PAR_TOT,
        CLRSKY_SFC_PAR_TOT, ALLSKY_SFC_UVA, ALLSKY_SFC_UVB, 
        ALLSKY_SFC_UV_INDEX, CITY, LOAD_TIMESTAMP
    )
    FROM (
        SELECT 
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
            CURRENT_TIMESTAMP()
        FROM @BRONZE.AZURE_BRONZE_STAGE
    )
    PATTERN='.*solar_energy.*\\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FORMAT')
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;
    """
    
    try:
        result = hook.run(solar_energy_copy)
        results['solar_energy'] = result
        print(f"✓ SOLAR_ENERGY loaded successfully")
    except Exception as e:
        print(f"⚠ SOLAR_ENERGY load completed with warnings: {str(e)}")
        results['solar_energy'] = str(e)
    
    # Load WEATHER data
    print("\n3. Loading WEATHER data...")
    weather_copy = """
    COPY INTO BRONZE.WEATHER (
        CITY, DATE, T2M, T2MDEW, T2MWET, TS, T2M_RANGE, T2M_MAX, T2M_MIN,
        QV2M, RH2M, PRECTOTCORR, PS, WS10M, WS10M_MAX, WS10M_MIN, LOAD_TIMESTAMP
    )
    FROM (
        SELECT 
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            CURRENT_TIMESTAMP()
        FROM @BRONZE.AZURE_BRONZE_STAGE
    )
    PATTERN='.*weather.*\\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FORMAT')
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;
    """
    
    try:
        result = hook.run(weather_copy)
        results['weather'] = result
        print(f"✓ WEATHER loaded successfully")
    except Exception as e:
        print(f"⚠ WEATHER load completed with warnings: {str(e)}")
        results['weather'] = str(e)
    
    print("\n" + "=" * 60)
    print("DATA LOADING COMPLETE")
    print("=" * 60)
    
    # Query row counts
    print("\nVerifying row counts in Bronze tables:")
    count_query = """
    SELECT 'AIR_POLLUTION' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.AIR_POLLUTION
    UNION ALL
    SELECT 'SOLAR_ENERGY', COUNT(*) FROM BRONZE.SOLAR_ENERGY
    UNION ALL
    SELECT 'WEATHER', COUNT(*) FROM BRONZE.WEATHER;
    """
    
    try:
        counts = hook.run(count_query, handler=lambda cursor: cursor.fetchall())
        for table, count in counts:
            print(f"  {table}: {count:,} rows")
    except Exception as e:
        print(f"⚠ Could not verify row counts: {str(e)}")
    
    return results

# NOTE: Removed the unused 'import subprocess' and 'PROJECT_ROOT' definition.

with DAG(
    'egyptian_weather_data_pipeline',
    default_args=default_args,
    description='Egyptian Weather Data Engineering Pipeline - Batch Processing',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'egypt', 'batch', 'production'],
    doc_md="""
    # Egyptian Weather Data Pipeline
    
    This pipeline processes weather, air pollution, and solar energy data for Egypt.
    
    ## Pipeline Flow:
    1. Upload new files from new_data/ to Azure (if any)
    2. Verify files in Azure Bronze directory
    3. Load data from Azure to Snowflake Bronze tables
    4. Transform to Silver layer (cleaned data)
    5. Transform to Gold layer (dimensional model)
    6. Run data quality tests
    
    ## Data Coverage:
    - Weather: 1981-2025 (daily, 27 governorates)
    - Air Pollution: 2021-2025 (hourly, 11 cities)
    - Solar Energy: 2006-2025 (hourly, 7 cities)
    """,
) as dag:
    
    upload_new_files = PythonOperator(
        task_id='upload_new_files_to_azure',
        python_callable=upload_new_files_to_azure,
        execution_timeout=timedelta(minutes=30),
    )
    
    check_files = PythonOperator(
        task_id='check_files_in_azure',
        python_callable=check_files_in_azure,
        execution_timeout=timedelta(minutes=5),
    )
    
    load_data = PythonOperator(
        task_id='load_data_from_azure',
        python_callable=load_data_from_azure,
        execution_timeout=timedelta(hours=1),
    )
    
    dbt_silver = DockerOperator(
        task_id='dbt_run_silver',
        image='batch_pipeline-dbt:latest',
        api_version='auto',
        auto_remove=True,
        command='bash -c "cd /usr/app/dbt/egypt_weather && dbt run --models silver"',
        docker_url='unix://var/run/docker.sock',
        network_mode='batch_pipeline_airflow_network',
        mounts=[
            Mount(
                source=os.getenv('HOST_DBT_PROJECT_PATH'),
                target='/usr/app/dbt/egypt_weather',
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=45),
        force_pull=False,
        environment={
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
            'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'),
        },
    )
    
    dbt_gold = DockerOperator(
        task_id='dbt_run_gold',
        image='batch_pipeline-dbt:latest',
        api_version='auto',
        auto_remove=True,
        command='bash -c "cd /usr/app/dbt/egypt_weather && dbt run --models gold --exclude tag:run_once"',
        docker_url='unix://var/run/docker.sock',
        network_mode='batch_pipeline_airflow_network',
        mounts=[
            Mount(
                source=os.getenv('HOST_DBT_PROJECT_PATH'),
                target='/usr/app/dbt/egypt_weather',
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=30),
        force_pull=False,
        environment={
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
            'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'),
        },
    )
    
    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='batch_pipeline-dbt:latest',
        api_version='auto',
        auto_remove=True,
        command='bash -c "cd /usr/app/dbt/egypt_weather && dbt test --store-failures"',
        docker_url='unix://var/run/docker.sock',
        network_mode='batch_pipeline_airflow_network',
        mounts=[
            Mount(
                source=os.getenv('HOST_DBT_PROJECT_PATH'),
                target='/usr/app/dbt/egypt_weather',
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=20),
        force_pull=False,
        environment={
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
            'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'),
        },
    )
    # flow
    upload_new_files >> check_files >> load_data >> dbt_silver >> dbt_gold >> dbt_test
