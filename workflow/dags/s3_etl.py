from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
from airflow.models import Variable
import os
import logging

from tasks.extract_json_data import extract_json_data
from tasks.transform_json_data import transform_json_data
from tasks.load_to_mongodb import load_to_mongodb
from tasks.extract_mongo_data import extract_mongo_data
from tasks.transform_mongo_data import transform_mongo_data
from tasks.load_mongo_data import load_mongo_data
from utils.telegram_alert import notify_on_failure, notify_on_success, notify_on_retry

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Calculate the start date for the last 5 days
start_date = datetime.now() - timedelta(days=5)

# DAG-level variables
DAG_ID = os.path.basename(__file__).replace(".py", "")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': start_date,  # Start date = 5 days ago
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_on_failure,
    'on_success_callback': notify_on_success,
    'on_retry_callback': notify_on_retry,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG for incremental processing and backfilling',
    tags=["ETL", "Incremental Processing", "Backfilling"],
    schedule_interval='0 19 * * *',  # Daily at 7 PM
    catchup=True,  # Enable backfilling
)


# --------------------- Define tasks
# ------- ETL json from s3
extract_json_data_from_s3 = PythonOperator(
    task_id='extract_json_data',
    python_callable=extract_json_data,
    provide_context=True,
    dag=dag
)

transform_json_data_from_s3 = PythonOperator(
    task_id='transform_json_data',
    python_callable=transform_json_data,
    # op_args=[extract_json_data.output],
    provide_context=True,
    dag=dag
)

load_json_to_mongo_task = PythonOperator(
    task_id='load_json_data',
    python_callable=load_to_mongodb,
    # op_args=[transform_json_data.output],
    op_kwargs={'db_name': 'videos', 'collection_name': 'videos'},
    provide_context=True,
    dag=dag
)

# ------- ETL mongo to clickhouse
extract_mongo_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_mongo_data,
    provide_context=True,
    dag=dag,
)

transform_mongo_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_mongo_data,
    # op_args=[extract_mongo_task.output],
    provide_context=True,
    dag=dag,
)

load_mongo_task = PythonOperator(
    task_id='load_data',
    python_callable=load_mongo_data,
    # op_args=[transform_mongo_task.output],
    provide_context=True,
    dag=dag,
)

# Define task dependencies
# extract_json_data_from_s3 >> transform_json_data_from_s3 >> load_json_to_mongo_task
# extract_mongo_task >> transform_mongo_task >> load_mongo_task

json_data_list = extract_json_data()
transformed_json_data = transform_json_data(json_data_list)
load_to_mongodb(transformed_json_data)

mongo_data = extract_mongo_data()
transformed_mongo_data = transform_mongo_data(mongo_data)
load_mongo_data(transformed_mongo_data)
