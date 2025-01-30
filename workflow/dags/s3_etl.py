import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from tasks.extract_json_data import extract_from_s3_and_transform_data
from tasks.extract_mongo_data import extract_mongo_data
from tasks.transform_mongo_data import transform_mongo_data
from tasks.load_mongo_data import load_mongo_data
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
@dag(default_args=default_args, schedule_interval='0 19 * * *', catchup=True,
     start_date=pendulum.now().subtract(days=5),
     description='DAG for incremental processing and backfilling', tags=["ETL", "Incremental Processing", "Backfilling"])
def s3_etl():

    @task()
    def extract_json_data_and_transform_task(**kwargs):
        return extract_from_s3_and_transform_data(**kwargs)

    # @task()
    # def extract_mongo_task(**kwargs) -> list[dict]:
    #     return extract_mongo_data(**kwargs)

    # @task()
    # def transform_mongo_task(mongo_data: list[dict], **kwargs) -> list[dict]:
    #     return transform_mongo_data(mongo_data, **kwargs)

    # @task()
    # def load_mongo_task(transformed_mongo_data: list[dict], **kwargs) -> None:
    #     return load_mongo_data(transformed_mongo_data, **kwargs)

    # Define task dependencies
    extract_json_data_and_transform_task()

    # mongo_data = extract_mongo_task()
    # transformed_mongo_data = transform_mongo_task(mongo_data)
    # load_mongo_task(transformed_mongo_data)


# Instantiate the DAG
dag = s3_etl()
