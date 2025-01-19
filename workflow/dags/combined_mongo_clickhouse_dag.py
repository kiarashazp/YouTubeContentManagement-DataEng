from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from clickhouse_driver import Client
from airflow.models import Variable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_clickhouse_schema():
    client = Client(host='clickhouse', user='airflow', password='airflow')

    client.execute('CREATE DATABASE IF NOT EXISTS bronze')

    client.execute('''
    CREATE TABLE IF NOT EXISTS bronze.videos (
        id String,
        owner_username String,
        owner_id String,
        title String,
        tags Nullable(String),
        uid String,
        visit_count Int64,
        owner_name String,
        duration Int32,
        comments Nullable(String),
        like_count Nullable(Int64),
        is_deleted Bool,
        created_at Int64,
        expire_at Int64,
        update_count Int32
    ) ENGINE = MergeTree() PRIMARY KEY (id) ORDER BY id
    ''')

    logger.info("ClickHouse schema created successfully.")
    client.disconnect()


def read_and_load(**kwargs):
    batch_size = int(Variable.get("mongo_batch_size", default_var=1000))
    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()
    clickhouse_client = Client(host='clickhouse', user='airflow', password='airflow')

    db = client['videos']
    collection = db['videos']
    cursor = collection.find().batch_size(batch_size)

    batch_number = 0
    while cursor.alive:
        batch = []
        for _ in range(batch_size):
            try:
                doc = cursor.next()
                # Extract the object data
                obj_data = doc.get('object', {})

                # Create a processed document with the correct field access
                processed_doc = {
                    'id': str(doc['_id']),  # Use MongoDB _id as the primary id
                    'owner_username': obj_data.get('owner_username', ''),
                    'owner_id': obj_data.get('owner_id', ''),
                    'title': obj_data.get('title', ''),
                    'tags': obj_data.get('tags', ''),
                    'uid': obj_data.get('uid', ''),
                    'visit_count': obj_data.get('visit_count', 0),
                    'owner_name': obj_data.get('owner_name', ''),
                    'duration': obj_data.get('duration', 0),
                    'comments': obj_data.get('comments', ''),
                    'like_count': obj_data.get('like_count', 0),
                    'is_deleted': obj_data.get('is_deleted', False),
                    'created_at': obj_data.get('created_at', 0),
                    'expire_at': obj_data.get('expire_at', 0),
                    'update_count': obj_data.get('update_count', 0)
                }
                batch.append(processed_doc)
            except StopIteration:
                break

        if batch:
            logger.info(f"Batch {batch_number} retrieved from MongoDB: {batch}")

            # Insert documents into ClickHouse
            try:
                clickhouse_client.execute(
                    'INSERT INTO bronze.videos VALUES',
                    [(
                        doc['id'],
                        doc['owner_username'],
                        doc['owner_id'],
                        doc['title'],
                        doc['tags'],
                        doc['uid'],
                        doc['visit_count'],
                        doc['owner_name'],
                        doc['duration'],
                        doc['comments'],
                        doc['like_count'],
                        doc['is_deleted'],
                        doc['created_at'],
                        doc['expire_at'],
                        doc['update_count']
                    ) for doc in batch]
                )
                logger.info(f"Batch {batch_number} inserted into ClickHouse.")
                batch_number += 1
            except Exception as ve:
                logger.info(f"--------- error {ve}")
    clickhouse_count = clickhouse_client.execute('SELECT count(*) FROM bronze.videos')
    logger.info(f"Total records in ClickHouse: {clickhouse_count}")

    client.close()
    clickhouse_client.disconnect()


# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
}

# Define the DAG
with DAG('combined_mongo_clickhouse_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    create_schema_task = PythonOperator(
        task_id='create_clickhouse_schema',
        python_callable=create_clickhouse_schema,
    )

    read_and_load_task = PythonOperator(
        task_id='read_and_load_from_mongo_to_clickhouse',
        python_callable=read_and_load,
        provide_context=True,
    )

# Set task dependencies
create_schema_task >> read_and_load_task
