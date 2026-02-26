# E-Commerce Real-Time Data Platform

A production-grade data engineering project simulating a real-time e-commerce analytics platform. Built with modern open-source tools running entirely locally with no cloud costs.

## Architecture

\\\
Event Producer (Python)
        |
        v
Apache Kafka (Docker)
        |
        v
Spark Structured Streaming
        |
        v
Delta Lake - Bronze Layer (Raw Events)
        |
        v
Delta Lake - Silver Layer (Cleaned & Enriched)
        |
        v
Delta Lake - Gold Layer (Business Metrics)
        |
        v
DuckDB + dbt (SQL Models & Tests)
        |
        v
Apache Airflow (Orchestration)
        +
Great Expectations (Data Quality)
        +
Terraform (IaC - AWS)
\\\

## Tech Stack

| Layer | Technology |
|---|---|
| Event Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Storage | Delta Lake (Medallion Architecture) |
| Warehouse | DuckDB |
| Transformation | dbt-core + dbt-duckdb |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Infrastructure | Terraform (AWS) |
| Language | Python 3.12, SQL |

## Project Structure

\\\
ecommerce-data-platform/
+-- producer/               # Kafka event producer
¦   +-- event_producer.py   # Simulates real-time e-commerce events
+-- spark_streaming/        # Spark jobs
¦   +-- bronze_consumer.py  # Kafka -> Bronze Delta Lake
¦   +-- silver_processor.py # Bronze -> Silver (cleaning)
¦   +-- gold_processor.py   # Silver -> Gold (aggregations)
+-- dbt_project/            # dbt transformation layer
¦   +-- load_to_duckdb.py   # Loads Gold layer into DuckDB
¦   +-- ecommerce_dbt/
¦       +-- models/
¦       ¦   +-- staging/    # Staging models + tests
¦       ¦   +-- marts/      # Business-facing models
¦       +-- dbt_project.yml
+-- airflow/                # Orchestration
¦   +-- dags/
¦   ¦   +-- ecommerce_pipeline.py
¦   +-- docker-compose-airflow.yml
+-- great_expectations/     # Data quality checks
¦   +-- data_quality_checks.py
+-- terraform/              # AWS infrastructure as code
¦   +-- main.tf
¦   +-- variables.tf
¦   +-- outputs.tf
+-- data/
¦   +-- bronze/             # Raw events
¦   +-- silver/             # Cleaned events
¦   +-- gold/               # Business metrics
+-- docker-compose.yml      # Kafka + Zookeeper
\\\

## Data Pipeline

### Bronze Layer
Raw e-commerce events streamed from Kafka and written to Delta Lake with no transformations. Events include page views, add-to-cart, purchases, and refunds.

### Silver Layer
Cleaned and enriched data. Transformations include deduplication, timestamp parsing, null filtering, event type standardization, and derived columns like price_bucket and is_purchase flag.

### Gold Layer
Three business metric tables:
- revenue_by_product: Total revenue, orders, avg order value per product
- user_summary: Per-user purchase history and customer segmentation
- conversion_funnel: Page view to purchase conversion rates

## dbt Models

Five models with 9 automated tests:
- stg_revenue_by_product
- stg_user_summary
- stg_conversion_funnel
- mart_top_products
- mart_customer_segments

## Data Quality

10 automated checks covering null validation, referential integrity, business logic rules, and funnel ordering. Results saved to JSON report.

## How to Run Locally

### Prerequisites
- Python 3.12+
- Docker Desktop
- Java 17

### 1. Start Kafka
\\\ash
docker compose up -d
\\\

### 2. Install dependencies
\\\ash
pip install -r requirements.txt
\\\

### 3. Start event producer
\\\ash
python producer/event_producer.py
\\\

### 4. Start Spark streaming (new terminal)
\\\ash
python spark_streaming/bronze_consumer.py
\\\

### 5. Run batch processors
\\\ash
python spark_streaming/silver_processor.py
python spark_streaming/gold_processor.py
\\\

### 6. Load to DuckDB and run dbt
\\\ash
python dbt_project/load_to_duckdb.py
cd dbt_project/ecommerce_dbt
dbt run
dbt test
\\\

### 7. Run data quality checks
\\\ash
python great_expectations/data_quality_checks.py
\\\

### 8. Start Airflow
\\\ash
cd airflow
docker compose -f docker-compose-airflow.yml up -d
\\\
Open http://localhost:8080 (admin/admin)

## Key Design Decisions

**Why Delta Lake over plain Parquet?**
Delta Lake provides ACID transactions, schema enforcement, and time travel — critical for production data pipelines where late-arriving data and schema changes are common.

**Why DuckDB for the warehouse layer?**
DuckDB is an embedded analytical database that runs in-process with no server required. For local development it provides full SQL capabilities with excellent performance on Parquet files.

**Why the Medallion Architecture?**
Separating raw, cleaned, and aggregated data into Bronze/Silver/Gold layers makes pipelines easier to debug, replay, and maintain. Each layer has a clear contract and can be reprocessed independently.

**Why Airflow for orchestration?**
Airflow's DAG-based model makes pipeline dependencies explicit and observable. The scheduler, retry logic, and UI are production patterns used at scale companies.

## Author
Ahalya Reddy Choda
