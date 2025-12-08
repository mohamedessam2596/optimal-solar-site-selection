FROM python:3.11-slim
 
LABEL maintainer="Weather Pipeline Team"
LABEL description="Kafka Producer for Egypt Weather Data"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY producer.py .
COPY partition_strategy.py .
COPY governorates.json .

# Create logs directory
RUN mkdir -p logs

# Set environment variables with defaults
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=egypt_weather_raw
ENV FETCH_INTERVAL_SECONDS=300
ENV TIMEOUT_SECONDS=10
ENV PYTHONUNBUFFERED=1

# Run the producer
CMD ["python", "-u", "producer.py"]
