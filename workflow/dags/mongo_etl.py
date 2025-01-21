import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from airflow.models import Variable
from airflow.models.param import Param
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG-level variables
DAG_ID = os.path.basename(__file__).replace(".py", "")
_SQL_DIR = os.path.join(
    os.path.dirname(__file__), '../../sql'
)

# Define default_args
default_args = { 
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1, 
    # 'retry_delay': timedelta(seconds=30),
    } 

def create_clickhouse_schema():
    client = Client(host='clickhouse', user='airflow', password='airflow')
    
    client.execute('CREATE DATABASE IF NOT EXISTS bronze')
    
    client.execute('''
    CREATE TABLE IF NOT EXISTS bronze.videos (
        id String,
        owner_username String,
        owner_id String,
        title String,
        tags Nullable(Array(String)),
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


def extract(**kwargs):
    batch_size = int(Variable.get("mongo_batch_size", default_var=1000))
    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()    
    # Access database and collection names from kwargs 
    db_name = kwargs.get('db_name', 'videos') 
    collection_name = kwargs.get('collection_name', 'videos')
    db = client[db_name]
    collection = db[collection_name] 
    cursor = collection.find().batch_size(batch_size)

    batch_number = 0
    processed_ids = set()
    batches = []

    while cursor.alive:
        batch = []
        for _ in range(batch_size):
            try:
                doc = cursor.next()
                obj_data = doc.get('object', {})
                if str(doc['_id']) not in processed_ids:
                    batch.append(obj_data)
                    processed_ids.add(str(doc['_id']))
            except StopIteration:
                break
            batch_number += 1
        batches.append(batch)

        client.close()
        return batches


def transform(batches: list[dict], **kwargs) -> list[dict]:
    """
    Transform the data
    Args:
        batches (list[dict]): Batch of extracted documents
    Returns:
        list[dict]: Transformed documents ready for loading
    """
    transformed_batch = []
    
    for doc in batches:
        transformed_doc = {
            'video_id': str(doc.get('id', '')),
            'owner_username': doc.get('owner_username', ''),
            'owner_id': doc.get('owner_id', ''),
            'title': doc.get('title', ''),
            'tags': doc.get('tags', ''),
            'uid': doc.get('uid', ''),
            'visit_count': doc.get('visit_count', 0),
            'owner_name': doc.get('owner_name', ''),
            'duration': doc.get('duration', 0),
            'posted_date': doc.get('posted_date', '1970-01-01'),
            'sdate_rss': doc.get('sdate_rss', '1970-01-01'),
            'comments': doc.get('comments', ''),
            'like_count': doc.get('like_count', 0),
            'is_deleted': doc.get('is_deleted', False),
            'created_at': doc.get('created_at', 0),
            'expire_at': doc.get('expire_at', 0),
            'update_count': doc.get('update_count', 0)
        }
        transformed_batch.append(transformed_doc)
    
    return transformed_batch

def load(transformed_batch: list[dict]):
    """
    Load the transformed data into ClickHouse
    Args:
        transformed_batch (list[dict]): Transformed documents
    """
    clickhouse_client = Client(host='clickhouse', user='airflow', password='airflow')
    
    field_names = [
        'id', 'owner_username', 'owner_id', 'title', 'tags', 'uid', 'visit_count',
        'owner_name', 'duration', 'comments', 'like_count', 'is_deleted', 
        'created_at', 'expire_at', 'update_count'
    ]
    field_names_sql = ",".join(field_names)

    try:
        clickhouse_client.execute(
            f'''
            INSERT INTO bronze.videos ({field_names_sql}) VALUES
            ''',
            [
                tuple(doc[field] for field in field_names)
                for doc in transformed_batch
            ]
        )
        logger.info(f"Batch inserted into ClickHouse.")
    except Exception as ve:
        logger.error(f"Error inserting batch into ClickHouse: {ve}")
    
    clickhouse_count = clickhouse_client.execute('SELECT count(*) FROM bronze.videos')
    logger.info(f"Total records in ClickHouse: {clickhouse_count}")
    clickhouse_client.disconnect()


# Define the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    template_searchpath=[_SQL_DIR],
    tags=["ETL", "ClickHouse", "MongoDB"]
) as dag:

    create_schema_task = PythonOperator(
        task_id='create_clickhouse_schema',
        python_callable=create_clickhouse_schema,
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        op_kwargs={'db_name': 'videos', 'collection_name': 'videos'}
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        op_args=[extract_task.output],
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        op_args=[transform_task.output]
    )

create_schema_task >> extract_task >> transform_task >> load_task








