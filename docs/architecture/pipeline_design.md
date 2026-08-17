# Retail Analytics ELT Pipeline Architecture

## Data Sources

- Orders
- Customers
- Products
- Returns
- Inventory

## Data Flow

GCS Landing
→ BigQuery Raw
→ dbt Staging
→ dbt Warehouse
→ dbt Marts
→ Business DQ
→ Notifications