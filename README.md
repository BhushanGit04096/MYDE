# Retail Analytics ELT Pipeline

## Tech Stack

- GCS
- BigQuery
- dbt
- Airflow
- Data Quality Checks
- Notifications

## Pipeline Flow

GCS Landing
→ BigQuery Raw
→ dbt Staging
→ dbt Warehouse
→ dbt Marts
→ Data Quality Checks
→ Notifications