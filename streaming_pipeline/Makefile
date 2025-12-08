# Makefile for Egypt Weather Streaming Pipeline
# Works on Linux, macOS, and Windows (with make installed)
 
.PHONY: help setup build up down logs clean restart status

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
RED := \033[0;31m
YELLOW := \033[1;33m
NC := \033[0m

help: ## Show this help message
	@echo "$(BLUE)Egypt Weather Streaming Pipeline$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## Initial setup - create directories and copy env file
	@echo "$(BLUE)Setting up project structure...$(NC)"
	@mkdir -p producer spark-jobs airflow/dags airflow/logs airflow/plugins
	@mkdir -p init-db conduktor grafana/provisioning/datasources grafana/provisioning/dashboards grafana/dashboards
	@mkdir -p logs/producer logs/spark
	@if [ ! -f .env ]; then cp .env.example .env; echo "$(GREEN)Created .env file - please update with your API key$(NC)"; fi
	@echo "$(GREEN)Setup complete!$(NC)"

build: ## Build all Docker images
	@echo "$(BLUE)Building Docker images...$(NC)"
	docker-compose build
	@echo "$(GREEN)Build complete!$(NC)"

up: ## Start all services
	@echo "$(BLUE)Starting services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo ""
	@echo "Access points:"
	@echo "  Conduktor:  http://localhost:8088 (admin@conduktor.io/admin)"
	@echo "  Airflow:    http://localhost:8081 (admin/admin)"
	@echo "  Spark UI:   http://localhost:8080"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

down: ## Stop all services
	@echo "$(BLUE)Stopping services...$(NC)"
	docker-compose down
	@echo "$(GREEN)Services stopped!$(NC)"

logs: ## View logs from all services
	docker-compose logs -f

logs-producer: ## View producer logs
	docker-compose logs -f weather-producer

logs-spark: ## View Spark master logs
	docker-compose logs -f spark-master

logs-kafka: ## View Kafka logs
	docker-compose logs -f kafka

logs-conduktor: ## View Conduktor Console logs
	docker-compose logs -f conduktor-console

logs-airflow: ## View Airflow scheduler logs
	docker-compose logs -f airflow-scheduler

status: ## Check status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@docker-compose ps

restart: down up ## Restart all services

restart-producer: ## Restart only the producer
	@echo "$(BLUE)Restarting producer...$(NC)"
	docker-compose restart weather-producer

restart-conduktor: ## Restart Conduktor Console
	@echo "$(BLUE)Restarting Conduktor Console...$(NC)"
	docker-compose restart conduktor-console

clean: ## Remove all containers, volumes, and images
	@echo "$(RED)WARNING: This will remove all data and images!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v --rmi all; \
		echo "$(GREEN)Cleanup complete!$(NC)"; \
	fi

shell-producer: ## Open shell in producer container
	docker exec -it weather-producer /bin/bash

shell-spark: ## Open shell in Spark master container
	docker exec -it spark-master /bin/bash

shell-db: ## Open PostgreSQL shell
	docker exec -it timescaledb psql -U weather_user -d weather_db

kafka-topics: ## List Kafka topics
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-describe: ## Describe Kafka topic
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic egypt_weather_raw

kafka-consume: ## Consume messages from Kafka (last 10)
	docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic egypt_weather_raw --from-beginning --max-messages 10

db-query: ## Run sample database query
	docker exec timescaledb psql -U weather_user -d weather_db -c "SELECT governorate, kelvin_to_celsius(main_temp) as temp_c, weather_main, timestamp FROM weather_data ORDER BY timestamp DESC LIMIT 10;"

db-query-celsius: ## View latest data in Celsius
	docker exec timescaledb psql -U weather_user -d weather_db -c "SELECT * FROM weather_data_celsius LIMIT 10;"

db-stats: ## Show database statistics
	docker exec timescaledb psql -U weather_user -d weather_db -c "SELECT * FROM partition_stats;"

db-gov-stats: ## Show governorate statistics
	docker exec timescaledb psql -U weather_user -d weather_db -c "SELECT * FROM governorate_temperature_stats ORDER BY avg_temp DESC;"

db-latest: ## View latest weather per governorate
	docker exec timescaledb psql -U weather_user -d weather_db -c "SELECT * FROM latest_weather;"

submit-spark: ## Submit Spark streaming job
	@echo "$(BLUE)Submitting Spark job...$(NC)"
	docker exec spark-master /bin/bash /opt/spark-jobs/submit_job.sh

conduktor-ui: ## Open Conduktor Console UI in browser
	@echo "Opening Conduktor Console..."
	@python -m webbrowser http://localhost:8088 || xdg-open http://localhost:8088 || open http://localhost:8088

spark-ui: ## Open Spark UI in browser
	@echo "Opening Spark UI..."
	@python -m webbrowser http://localhost:8080 || xdg-open http://localhost:8080 || open http://localhost:8080

airflow-ui: ## Open Airflow UI in browser
	@echo "Opening Airflow UI..."
	@python -m webbrowser http://localhost:8081 || xdg-open http://localhost:8081 || open http://localhost:8081

grafana-ui: ## Open Grafana UI in browser
	@echo "Opening Grafana UI..."
	@python -m webbrowser http://localhost:3000 || xdg-open http://localhost:3000 || open http://localhost:3000

test-api: ## Test OpenWeather API connection
	@echo "$(BLUE)Testing OpenWeather API...$(NC)"
	@if [ -f .env ]; then \
		. .env && curl -s "https://api.openweathermap.org/data/2.5/weather?lat=30.0444&lon=31.2357&appid=$$OPENWEATHER_API_KEY&units=metric" | jq '.main.temp'; \
	else \
		echo "$(RED).env file not found$(NC)"; \
	fi

verify-all: ## Verify all services are working
	@echo "$(BLUE)Verifying pipeline health...$(NC)"
	@echo ""
	@echo "$(YELLOW)1. Checking Kafka...$(NC)"
	@docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092 > /dev/null 2>&1 && echo "$(GREEN)✓ Kafka is healthy$(NC)" || echo "$(RED)✗ Kafka has issues$(NC)"
	@echo ""
	@echo "$(YELLOW)2. Checking Database...$(NC)"
	@docker exec timescaledb pg_isready -U weather_user > /dev/null 2>&1 && echo "$(GREEN)✓ Database is healthy$(NC)" || echo "$(RED)✗ Database has issues$(NC)"
	@echo ""
	@echo "$(YELLOW)3. Checking Conduktor Console...$(NC)"
	@curl -sf http://localhost:8088/health > /dev/null 2>&1 && echo "$(GREEN)✓ Conduktor Console is healthy$(NC)" || echo "$(RED)✗ Conduktor Console has issues$(NC)"
	@echo ""
	@echo "$(YELLOW)4. Service URLs:$(NC)"
	@echo "   Conduktor Console: http://localhost:8088"
	@echo "   Airflow: http://localhost:8081"
	@echo "   Spark UI: http://localhost:8080"
	@echo "   Grafana: http://localhost:3000"
