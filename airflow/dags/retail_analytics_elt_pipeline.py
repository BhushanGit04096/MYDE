#replace the destination dataset path and the DBT model path till MYDE/dbt/retail_analytics_dbt and change the project details in DQ sqls

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime

# ==========================================================
# CONFIGURABLE ENVIRONMENT SETTINGS
# ==========================================================
GCP_PROJECT_ID = "playground-s-11-fc547a0e"  # Update with your GCP Project ID
RAW_DATASET = "retail_raw"                     # Update with your target BigQuery dataset
GCS_BUCKET = "retail-data-bhushandata"           # Update with your target GCS bucket

# Base directory paths
BASE_DIR = "/home/cloud_user_p_87cb0864/MYDE"
DBT_PROJECT_DIR = f"{BASE_DIR}/dbt/retail_analytics_dbt"
SQL_DIR = f"{BASE_DIR}/airflow/include/sql"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "email": ["nagabhushancherry@gmail.com", "deepaspaul97@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False
}

FILE_DATE = "{{ ds }}"

with DAG(
    dag_id="retail_analytics_elt_pipeline",
    description="Production-style Retail Analytics ELT Pipeline using GCS, BigQuery, Airflow and dbt",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 18),
    catchup=False,
    tags=["retail", "elt", "gcp", "bigquery", "dbt"]
) as dag:

    # ==========================================================
    # INGESTION GROUP
    # ==========================================================

    with TaskGroup(group_id="ingestion_group") as ingestion_group:

        # ------------------------------------------------------
        # ORDERS
        # ------------------------------------------------------

        orders_sensor = GCSObjectExistenceSensor(
            task_id="orders_sensor",
            bucket=GCS_BUCKET,
            object=f"landing/orders/date={FILE_DATE}/orders.csv",
            poke_interval=60,
            timeout=3600
        )

        orders_load = GCSToBigQueryOperator(
            task_id="orders_load",
            bucket=GCS_BUCKET,
            source_objects=[
                f"landing/orders/date={FILE_DATE}/orders.csv"
            ],
            destination_project_dataset_table=
                f"{GCP_PROJECT_ID}.{RAW_DATASET}.raw_orders",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True,
            time_partitioning={
                "type": "DAY",
                "field": "order_date"
            },
            cluster_fields=[
                "customer_id",
                "product_id"
            ]
        )

        orders_sensor >> orders_load

        # ------------------------------------------------------
        # CUSTOMERS
        # ------------------------------------------------------

        customers_sensor = GCSObjectExistenceSensor(
            task_id="customers_sensor",
            bucket=GCS_BUCKET,
            object=f"landing/customers/date={FILE_DATE}/customers.csv",
            poke_interval=60,
            timeout=3600
        )

        customers_load = GCSToBigQueryOperator(
            task_id="customers_load",
            bucket=GCS_BUCKET,
            source_objects=[
                f"landing/customers/date={FILE_DATE}/customers.csv"
            ],
            destination_project_dataset_table=
                f"{GCP_PROJECT_ID}.{RAW_DATASET}.raw_customers",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        customers_sensor >> customers_load

        # ------------------------------------------------------
        # PRODUCTS
        # ------------------------------------------------------

        products_sensor = GCSObjectExistenceSensor(
            task_id="products_sensor",
            bucket=GCS_BUCKET,
            object=f"landing/products/date={FILE_DATE}/products.csv",
            poke_interval=60,
            timeout=3600
        )

        products_load = GCSToBigQueryOperator(
            task_id="products_load",
            bucket=GCS_BUCKET,
            source_objects=[
                f"landing/products/date={FILE_DATE}/products.csv"
            ],
            destination_project_dataset_table=
                f"{GCP_PROJECT_ID}.{RAW_DATASET}.raw_products",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        products_sensor >> products_load

        # ------------------------------------------------------
        # RETURNS
        # ------------------------------------------------------

        returns_sensor = GCSObjectExistenceSensor(
            task_id="returns_sensor",
            bucket=GCS_BUCKET,
            object=f"landing/returns/date={FILE_DATE}/returns.csv",
            poke_interval=60,
            timeout=3600
        )

        returns_load = GCSToBigQueryOperator(
            task_id="returns_load",
            bucket=GCS_BUCKET,
            source_objects=[
                f"landing/returns/date={FILE_DATE}/returns.csv"
            ],
            destination_project_dataset_table=
                f"{GCP_PROJECT_ID}.{RAW_DATASET}.raw_returns",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True,
            time_partitioning={
                "type": "DAY",
                "field": "return_date"
            },
            cluster_fields=[
                "order_id"
            ]
        )

        returns_sensor >> returns_load

        # ------------------------------------------------------
        # INVENTORY
        # ------------------------------------------------------

        inventory_sensor = GCSObjectExistenceSensor(
            task_id="inventory_sensor",
            bucket=GCS_BUCKET,
            object=f"landing/inventory/date={FILE_DATE}/inventory.csv",
            poke_interval=60,
            timeout=3600
        )

        inventory_load = GCSToBigQueryOperator(
            task_id="inventory_load",
            bucket=GCS_BUCKET,
            source_objects=[
                f"landing/inventory/date={FILE_DATE}/inventory.csv"
            ],
            destination_project_dataset_table=
                f"{GCP_PROJECT_ID}.{RAW_DATASET}.raw_inventory",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True,
            time_partitioning={
                "type": "DAY",
                "field": "last_updated"
            },
            cluster_fields=[
                "product_id"
            ]
        )

        inventory_sensor >> inventory_load


    # ==========================================================
    # DBT GROUP
    # ==========================================================

    with TaskGroup(group_id="dbt_group") as dbt_group:

        dbt_staging = BashOperator(
            task_id="dbt_staging",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --select staging
            """
        )

        dbt_warehouse = BashOperator(
            task_id="dbt_warehouse",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --select warehouse
            """
        )

        dbt_marts = BashOperator(
            task_id="dbt_marts",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --select marts
            """
        )

        dbt_staging >> dbt_warehouse >> dbt_marts

    # ==========================================================
    # VALIDATION GROUP
    # ==========================================================

    with TaskGroup(group_id="validation_group") as validation_group:

        validate_dim_customers = BashOperator(
            task_id="validate_dim_customers",
            bash_command=f"""
            bq query --use_legacy_sql=false \
            < {SQL_DIR}/validate_dim_customers.sql
            """
        )

        validate_dim_products = BashOperator(
            task_id="validate_dim_products",
            bash_command=f"""
            bq query --use_legacy_sql=false \
            < {SQL_DIR}/validate_dim_products.sql
            """
        )

        validate_fact_orders = BashOperator(
            task_id="validate_fact_orders",
            bash_command=f"""
            bq query --use_legacy_sql=false \
            < {SQL_DIR}/validate_fact_orders.sql
            """
        )

        validate_fact_returns = BashOperator(
            task_id="validate_fact_returns",
            bash_command=f"""
            bq query --use_legacy_sql=false \
            < {SQL_DIR}/validate_fact_returns.sql
            """
        )

        validate_fact_inventory = BashOperator(
            task_id="validate_fact_inventory",
            bash_command=f"""
            bq query --use_legacy_sql=false \
            < {SQL_DIR}/validate_fact_inventory.sql
            """
        )

        (
            validate_dim_customers 
            >> validate_dim_products 
            >> validate_fact_orders 
            >> validate_fact_returns 
            >> validate_fact_inventory
        )

    # ==========================================================
    # ARCHIVE GROUP
    # ==========================================================

    with TaskGroup(group_id="archive_group") as archive_group:

        archive_orders = GCSToGCSOperator(
            task_id="archive_orders",
            source_bucket=GCS_BUCKET,
            source_object=f"landing/orders/date={FILE_DATE}/orders.csv",
            destination_bucket=GCS_BUCKET,
            destination_object=f"archive/orders/date={FILE_DATE}/orders.csv",
            move_object=True
        )

        archive_customers = GCSToGCSOperator(
            task_id="archive_customers",
            source_bucket=GCS_BUCKET,
            source_object=f"landing/customers/date={FILE_DATE}/customers.csv",
            destination_bucket=GCS_BUCKET,
            destination_object=f"archive/customers/date={FILE_DATE}/customers.csv",
            move_object=True
        )

        archive_products = GCSToGCSOperator(
            task_id="archive_products",
            source_bucket=GCS_BUCKET,
            source_object=f"landing/products/date={FILE_DATE}/products.csv",
            destination_bucket=GCS_BUCKET,
            destination_object=f"archive/products/date={FILE_DATE}/products.csv",
            move_object=True
        )

        archive_returns = GCSToGCSOperator(
            task_id="archive_returns",
            source_bucket=GCS_BUCKET,
            source_object=f"landing/returns/date={FILE_DATE}/returns.csv",
            destination_bucket=GCS_BUCKET,
            destination_object=f"archive/returns/date={FILE_DATE}/returns.csv",
            move_object=True
        )

        archive_inventory = GCSToGCSOperator(
            task_id="archive_inventory",
            source_bucket=GCS_BUCKET,
            source_object=f"landing/inventory/date={FILE_DATE}/inventory.csv",
            destination_bucket=GCS_BUCKET,
            destination_object=f"archive/inventory/date={FILE_DATE}/inventory.csv",
            move_object=True
        )

    pipeline_success_notification = BashOperator(
        task_id="pipeline_success_notification",
        bash_command="echo 'Retail Analytics ELT Pipeline Completed Successfully'"
    )

    # ==========================================================
    # DAG DEPENDENCIES
    # ==========================================================

    ingestion_group >> dbt_group >> validation_group >> archive_group >> pipeline_success_notification
