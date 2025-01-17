from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Task to read from MongoDB
def read_from_mongo(**kwargs):
    try:
        # You can adjust the batch size in the Airflow UI without changing the code
        batch_size = int(Variable.get("mongo_batch_size", default_var=10))
        mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
        client = mongo_hook.get_conn()
        db = client['videos']
        collection = db['videos']
        cursor = collection.find().batch_size(batch_size)

        batches = []
        batch_number = 0
        while cursor.alive:
            batch = []
            for i in range(batch_size):
                try:
                    doc = cursor.next()
                    batch.append(doc)
                except StopIteration:
                    break
            if batch:
                batches.append(batch)
                logger.info(f"Batch {batch_number} retrieved from MongoDB: {batch}")
                batch_number += 1

        kwargs['ti'].xcom_push(key='mongo_batches', value=batches)
        logger.info(f"Total batches pushed to XCom: {len(batches)}")

    except Exception as error:
        logger.error(f"Error extracting data from MongoDB: {error}")
        raise

    finally:
        client.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('test_read_from_mongo_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    read_task = PythonOperator(
        task_id='read_from_mongo',
        python_callable=read_from_mongo,
    )

