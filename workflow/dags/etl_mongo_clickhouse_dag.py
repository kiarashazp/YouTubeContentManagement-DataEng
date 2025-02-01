from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from utils.create_clickhouse_schema import create_clickhouse_schema
from tasks.mongo_clickhouse_etl import mongo_clickhouse_etl

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG-level variables
DAG_ID = os.path.basename(__file__).replace(".py", "")

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    DAG_ID,
    tags=["ETL", "Phase 1", "MongoDB", "ClickHouse", "Batch Processing"],
    default_args=default_args, 
    schedule_interval='@once', 
    catchup=False
    ) as dag:

    create_schema_task = PythonOperator(
        task_id='create_clickhouse_schema',
        python_callable=create_clickhouse_schema,
    )

    mongo_to_clickhouse_etl_task = PythonOperator(
        task_id='read_and_load_from_mongo_to_clickhouse',
        python_callable=mongo_clickhouse_etl,
        provide_context=True,
    )

# Set task dependencies
create_schema_task >> mongo_to_clickhouse_etl_task
