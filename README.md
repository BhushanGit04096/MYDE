# Retail Analytics ELT Pipeline

## Tech Stack

* GCS
* BigQuery
* dbt
* Airflow
* Data Quality Checks
* Notifications

## Pipeline Flow

GCS Landing
→ BigQuery Raw
→ dbt Staging
→ dbt Warehouse
→ dbt Marts
→ Data Quality Checks
→ Notifications







\## dbt Setup



Create:



C:\\Users\\<username>\\.dbt\\profiles.yml



Use the template:



dbt/retail\_analytics\_dbt/profiles.yml.example



Replace:



YOUR\_PROJECT\_ID



with your GCP Project ID.



Verify:



```bash

dbt debug

```

