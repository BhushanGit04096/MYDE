#replace the destination dataset path and the DBT model path till MYDE/dbt/retail_analytics_dbt



from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineering",
    "retries": 2
}

FILE_DATE = "{{ ds }}"

with DAG(
    dag_id="retail_analytics_elt_pipeline",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 18),
    catchup=False
) as dag:

    # ==========================================================
    # INGESTION GROUP
    # ==========================================================

    with TaskGroup(group_id="ingestion_group") as ingestion_group:

        # Orders

        orders_sensor = GCSObjectExistenceSensor(
            task_id="orders_sensor",
            bucket="retail-data-bhushande",
            object=f"landing/orders/date={FILE_DATE}/orders.csv",
            poke_interval=60,
            timeout=3600
        )

        orders_load = GCSToBigQueryOperator(
            task_id="orders_load",
            bucket="retail-data-bhushande",
            source_objects=[
                f"landing/orders/date={FILE_DATE}/orders.csv"
            ],
            destination_project_dataset_table="playground-s-11-82a9d55d.retail_raw.raw_orders",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        orders_sensor >> orders_load

        # Customers

        customers_sensor = GCSObjectExistenceSensor(
            task_id="customers_sensor",
            bucket="retail-data-bhushande",
            object=f"landing/customers/date={FILE_DATE}/customers.csv",
            poke_interval=60,
            timeout=3600
        )

        customers_load = GCSToBigQueryOperator(
            task_id="customers_load",
            bucket="retail-data-bhushande",
            source_objects=[
                f"landing/customers/date={FILE_DATE}/customers.csv"
            ],
            destination_project_dataset_table="playground-s-11-82a9d55d.retail_raw.raw_customers",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        customers_sensor >> customers_load

        # Products

        products_sensor = GCSObjectExistenceSensor(
            task_id="products_sensor",
            bucket="retail-data-bhushande",
            object=f"landing/products/date={FILE_DATE}/products.csv",
            poke_interval=60,
            timeout=3600
        )

        products_load = GCSToBigQueryOperator(
            task_id="products_load",
            bucket="retail-data-bhushande",
            source_objects=[
                f"landing/products/date={FILE_DATE}/products.csv"
            ],
            destination_project_dataset_table="playground-s-11-82a9d55d.retail_raw.raw_products",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        products_sensor >> products_load

        # Returns

        returns_sensor = GCSObjectExistenceSensor(
            task_id="returns_sensor",
            bucket="retail-data-bhushande",
            object=f"landing/returns/date={FILE_DATE}/returns.csv",
            poke_interval=60,
            timeout=3600
        )

        returns_load = GCSToBigQueryOperator(
            task_id="returns_load",
            bucket="retail-data-bhushande",
            source_objects=[
                f"landing/returns/date={FILE_DATE}/returns.csv"
            ],
            destination_project_dataset_table="playground-s-11-82a9d55d.retail_raw.raw_returns",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        returns_sensor >> returns_load

        # Inventory

        inventory_sensor = GCSObjectExistenceSensor(
            task_id="inventory_sensor",
            bucket="retail-data-bhushande",
            object=f"landing/inventory/date={FILE_DATE}/inventory.csv",
            poke_interval=60,
            timeout=3600
        )

        inventory_load = GCSToBigQueryOperator(
            task_id="inventory_load",
            bucket="retail-data-bhushande",
            source_objects=[
                f"landing/inventory/date={FILE_DATE}/inventory.csv"
            ],
            destination_project_dataset_table="playground-s-11-82a9d55d.retail_raw.raw_inventory",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )

        inventory_sensor >> inventory_load

# ==========================================================
# DBT GROUP
# ==========================================================

DBT_PROJECT_DIR = "/home/cloud_user_p_d70e59e8/MYDE/dbt/retail_analytics_dbt"

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

        validate_orders = BashOperator(
            task_id="validate_orders",
            bash_command="echo 'Validating fact_orders'"
        )

        validate_customers = BashOperator(
            task_id="validate_customers",
            bash_command="echo 'Validating dim_customers'"
        )

        validate_products = BashOperator(
            task_id="validate_products",
            bash_command="echo 'Validating dim_products'"
        )

    # ==========================================================
    # ARCHIVE GROUP
    # ==========================================================

    with TaskGroup(group_id="archive_group") as archive_group:

        archive_orders = BashOperator(
            task_id="archive_orders",
            bash_command="echo 'Archive orders file'"
        )

        archive_customers = BashOperator(
            task_id="archive_customers",
            bash_command="echo 'Archive customers file'"
        )

        archive_products = BashOperator(
            task_id="archive_products",
            bash_command="echo 'Archive products file'"
        )

        archive_returns = BashOperator(
            task_id="archive_returns",
            bash_command="echo 'Archive returns file'"
        )

        archive_inventory = BashOperator(
            task_id="archive_inventory",
            bash_command="echo 'Archive inventory file'"
        )

    # ==========================================================
    # DAG DEPENDENCIES
    # ==========================================================

    ingestion_group >> dbt_group >> validation_group >> archive_group