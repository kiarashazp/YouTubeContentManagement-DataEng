import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from tasks.etl_s3_to_mongodb import elt_json_to_mongodb
from utils.telegram_alert import notify_on_failure, notify_on_success, notify_on_retry
from clickhouse_driver import Client
from airflow.models import Variable


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

start_date = datetime.now() - timedelta(days=5)
DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_on_failure,
    'on_success_callback': notify_on_success,
    'on_retry_callback': notify_on_retry,
}


# Use the @dag decorator to define your DAG
@dag(default_args=default_args, schedule_interval='0 19 * * *', catchup=True, start_date=pendulum.now().subtract(days=5),
     description='DAG for incremental processing and backfilling', tags=["ETL", "Incremental Processing", "Backfilling"])
def s3_etl():
    @task()
    def extract_json_data_and_transform_task(**kwargs):
        return elt_json_to_mongodb(**kwargs)

    elt_json_to_mongodb()


# Instantiate the DAG
dag = s3_etl()
