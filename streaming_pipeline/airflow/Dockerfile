FROM apache/airflow:2.8.1-python3.11
 
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create directories
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
