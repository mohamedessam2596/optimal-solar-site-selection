# Egyptian Weather Data Engineering & Analytics Platform #

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![DBT](https://img.shields.io/badge/DBT-1.7.0-orange.svg)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.7.0-blue.svg)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-red.svg)](https://spark.apache.org/)

> **A comprehensive end-to-end data engineering solution for climate intelligence across Egypt**

Unified platform combining **44 years of historical weather data** (1981-2025) with **real-time monitoring** across all **27 Egyptian governorates**, delivering actionable climate insights for government, agriculture, energy, and public health sectors.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data Coverage](#data-coverage)
- [Project Main Structure Components](#project-main-structure-components)
- [Pipeline Details](#pipeline-details)
- [Data Model](#-data-model)
- [Dashboards & Visualizations](#dashboards--visualizations)
- [Team](#team)
- [License](#license)
- [Acknowledgments](#acknowledgments)

---

## Overview

This project main purpose is to build a scalable, hybrid data platform that processes **1.9M+ historical records** and provides **sub-30-second real-time monitoring** capabilities.

### Business Impact

| Stakeholder | Impact | Value |
|------------|--------|-------|
| 🏛️ **Government** | Policy decisions, disaster preparedness | Data-driven climate insights |
| 🌾 **Agriculture** | Crop planning, irrigation management | $500M+ potential savings annually |
| ⚡ **Energy Sector** | Solar farm planning, grid management | 20-30% efficiency gains |
| 🏥 **Healthcare** | Air quality alerts, disease prevention | $200M+ health cost reduction |
| 🏙️ **Urban Planning** | Smart city development, infrastructure | Regional climate profiles |
| ✈️ **Tourism** | Season planning, visitor safety | $100M+ revenue optimization |

### Key Achievements

- ✅ **1.9M+ historical records** processed across 44 years
- ✅ **27 governorates** with complete climate profiles
- ✅ **100% data quality** test pass rate (35/35 tests)
- ✅ **90%+ processing time reduction** with optimized incremental loading
- ✅ **Zero data loss** in production

---

## Key Features

### Lambda Architecture

**Batch Pipeline (Historical Analysis)**
- 44 years of weather history (1981-2025)
- 1.9M+ data points across weather, air quality, and solar energy
- Incremental loading
- Automated data quality testing

**Streaming Pipeline (Real-Time Monitoring)**
- 30-second update intervals
- All 27 governorates monitored simultaneously
- Kafka + Spark distributed processing

### Advanced Analytics

- 📊 Climate trend analysis and forecasting
- 🌡️ Temperature, humidity, and precipitation tracking
- 💨 Air quality monitoring (AQI, PM2.5, PM10, pollutants)
- ☀️ Solar energy potential assessment (UV index, irradiance)
- 🗺️ Regional comparative analysis
- 📈 Year-over-year change detection

### Data Quality & Governance

- Automated testing framework (DBT)
- Referential integrity enforcement
- Audit trails and lineage tracking
- Standardized transformations
- Schema evolution support

---

## Architecture

![Architecture Diagram](docs/architecture.png)

### Medallion Architecture

- **Bronze Layer**: Raw data preservation (audit trail)
- **Silver Layer**: Cleaned, standardized, business-ready data
- **Gold Layer**: Analytics-optimized dimensional model (Galaxy Schema)

---

## Technology Stack

### Data Ingestion & Storage
- **Azure Blob Storage** - Data lake for raw CSV files
- **Apache Kafka** - Real-time message broker (KRaft mode)
- **Snowflake** - Cloud data warehouse
- **TimescaleDB** - Time-series database

### Processing & Transformation
- **Apache Airflow** - Workflow orchestration
- **DBT Core** - SQL-based transformations
- **Apache Spark** - Distributed stream processing
- **Python** - Data engineering scripts

### Visualization & Monitoring
- **Power BI** - Interactive dashboards (batch data)
- **Grafana** - Real-time monitoring
- **Conduktor Console** - Kafka UI

### Infrastructure
- **Docker & Docker Compose** - Containerization (15 containers)
- **Git** - Version control

---

## Data Coverage

### Historical Data

| Dataset | Date Range | Frequency | Cities | Records |
|---------|-----------|-----------|--------|---------|
| **Weather** | 1981-2025 | Daily | 27 | 449,988 |
| **Air Pollution** | 2021-2025 | Hourly | 11 | 381,128 |
| **Solar Energy** | 2006-2025 | Hourly | 7 | 1,165,920 |
| **Total** | | | | **1,997,036** |

### Geographic Coverage (27 Governorates)

- **Greater Cairo**: Cairo, Giza, Qalyubia
- **Alexandria & North Coast**: Alexandria, Beheira, Kafr El Sheikh, Matrouh
- **Delta & Canal**: Dakahlia, Sharqia, Gharbia, Monufia, Damietta, Port Said, Ismailia, Suez
- **Upper Egypt**: Aswan, Luxor, Qena, Sohag, Asyut, Minya, Beni Suef, Fayoum
- **Frontier Governorates**: South Sinai, North Sinai, Red Sea, New Valley

### Real-Time Data

- **Source**: OpenWeather API
- **Frequency**: 30-second intervals
- **Coverage**: All 27 governorates
- **Metrics**: 30+ weather attributes per record

---

## Project Main Structure Components

```
egypt_weather_project/
├── batch_pipeline/
│   ├── airflow/
│   │   ├── dags/
│   │   │   └── egypt_weather_pipeline.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── dbt/
│   │   ├── egypt_weather/
│   │   │   ├── models/
│   │   │   │   ├── silver/           # 3 incremental models
│   │   │   │   └── gold/
│   │   │   │       ├── dimensions   # 2 run-once models
│   │   │   │       └── facts        # 3 incremental models
│   │   │   ├── tests/
│   │   │   ├── dbt_project.yml
│   │   │   └── profiles.yml
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── docker-compose.yml
│   └── .env
│
├── streaming_pipeline/
│   ├── spark/
│   │   ├── weather_processor.py
│   │   └── submit_job.sh
│   ├── airflow/
│   │   └── dags/
│   │       └── weather_pipeline_dag.py
│   ├── grafana/
│   │   └── dashboards/
│   ├── docker-compose.yml
│   └── .env
│
├── docs/
│   ├── architecture.png
│   ├── galaxy_schema.png
│   ├── batch_airflow_dag.png
│   ├── streaming_airflow_dag.png
│   ├── grafana_dashboard_screenshots/
│   └── powerbi_dashboard_screenshots/
│
├── .gitignore
└── README.md
```

---

## Pipeline Details

### Batch Pipeline Workflow

```
upload_new_files_to_azure
↓
check_files_in_azure
↓
load_data_from_azure (Snowflake COPY INTO)
↓
dbt_run_silver (Incremental models)
↓
dbt_run_gold (Run-once dimensions + Incremental facts)
↓
dbt_test (35 data quality tests)
```

![Batch Airflow Dag](docs/batch_airflow_dag.png)

### Streaming Pipeline Workflow

```
OpenWeather API → Kafka Producer → Kafka Broker (3 partitions)
                                         ↓
                                   Spark Streaming
                                         ↓
                                   TimescaleDB
                                         ↓
                                      Grafana
```

![Streaming Airflow Dag](docs/streaming_airflow_dag.png)

### DBT Models

**Silver Layer (Incremental):**
- `silver_weather.sql` - Daily weather data
- `silver_air_pollution.sql` - Hourly pollution data
- `silver_solar_energy.sql` - Hourly solar data

**Gold Layer - Dimensions (Run Once):**
- `dim_date.sql` - 18,263 dates (1981-2030)
- `dim_governorate.sql` - 27 governorates with regional classifications

**Gold Layer - Facts (Incremental):**
- `fact_weather.sql` - Daily weather metrics
- `fact_air_pollution.sql` - Hourly pollution metrics
- `fact_solar_energy.sql` - Hourly solar metrics

**Selective Execution:**
```bash
# First time - build everything
dbt run --full-refresh

# Regular runs - skip dimensions
dbt run --exclude tag:run_once

# Rebuild dimensions only (rare)
dbt run --models tag:run_once --full-refresh

# Run only facts
dbt run --models tag:incremental
```

---

## Data Model

### Galaxy Schema Design

![Galaxy Schema](docs/galaxy_schema.png)

**Key Design Decisions:**
- Run-once dimensions (tagged, skipped in regular runs)
- Incremental facts (process only new records)
- Hash-based surrogate keys (dbt_utils.generate_surrogate_key)
- Transform timestamp tracking for precise filtering
- Schema evolution support

---

## Dashboards & Visualizations

### Power BI Dashboards (Batch Data)

**1. Performance Overview**
![Performance Overview](docs/powerbi_dashboard_screenshots/Performance%20Overview.png)

**2. Cairo Performance Analysis**
![Cairo Performance](docs/powerbi_dashboard_screenshots/Cairo%20Performance%20Overview.png)

**3. Governorate Comparison**
![Governorate Comparison](docs/powerbi_dashboard_screenshots/Governorate%20Comparison.png)

**4. Governorate Comparison 2024**
![Governorate Comparison 2024](docs/powerbi_dashboard_screenshots/Governorate%20Comparison%202024.png)

**5. Location Decision Tool**
![Location Decision](docs/powerbi_dashboard_screenshots/Location%20Decision.png)

**6. Seasonal Analysis**
![Season Analysis](docs/powerbi_dashboard_screenshots/Season%20Analysis.png)

**7. New Valley Seasonal Analysis**
![New Valley Analysis](docs/powerbi_dashboard_screenshots/New%20Valley%20Seasonal%20Analysis.png)

**8. Aswan Performance Overview**
![Aswan Performance](docs/powerbi_dashboard_screenshots/Performance%20Overview%20Aswan.png)

### Grafana Dashboards (Real-Time Data)

![Grafana Dashboard 1](docs/grafana_dashboard_screenshots/1.png)

![Grafana Dashboard 2](docs/grafana_dashboard_screenshots/2.png)

![Grafana Dashboard 3](docs/grafana_dashboard_screenshots/3.png)

---

## Data Quality

### DBT Testing Framework

**Test Coverage:**
- Uniqueness: (all primary keys)
- Not Null: (required fields)
- Relationships: (referential integrity)
- **Total: 35 tests (100% pass rate)**

**Run Tests:**
```bash
docker exec dbt-service dbt test --store-failures
```

**Test Categories:**
```yaml
# Dimension tests
- unique + not_null on all keys
- not_null on required attributes

# Fact table tests
- unique + not_null on surrogate keys
- relationships to dimensions (foreign keys)
- not_null on key metrics
```

---

## Team

This project was created by a dedicated team of Data Engineering students:

* **Ahmed Mohammed Abdelbadie** — [https://www.linkedin.com/in/ahmedmo2001/](https://www.linkedin.com/in/ahmedmo2001/)
* **Ayman Sayed Ahmed** — [https://www.linkedin.com/in/ayman-sayed-shama/](https://www.linkedin.com/in/ayman-sayed-shama/)
* **Mahmoud Ayman Riad** — [https://www.linkedin.com/in/mahmoudaymanriyad/](https://www.linkedin.com/in/mahmoudaymanriyad/)
* **Mohammed Essam Elsadek** — [https://www.linkedin.com/in/mohamed-essam-3642441bb/](https://www.linkedin.com/in/mohamed-essam-3642441bb/)
* **Nahla Mohammed Hussein** — [https://www.linkedin.com/in/nahla-mohamed-khedr/](https://www.linkedin.com/in/nahla-mohamed-khedr/)

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **NASA POWER** - Historical weather data
- **OpenWeather** - Real-time weather API
- **Open Source Community** - Amazing tools and frameworks
