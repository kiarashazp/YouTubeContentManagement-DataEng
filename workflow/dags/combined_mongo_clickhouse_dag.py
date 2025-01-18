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

# Task to create schema in ClickHouse
def create_clickhouse_schema():
    client = Client(host='clickhouse', user='airflow', password='airflow')
    
    # Create the database if it doesn't exist
    client.execute('CREATE DATABASE IF NOT EXISTS bronze')
    
    # Create a table in ClickHouse
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
        posted_timestamp Int32,
        comments Nullable(String),
        like_count Nullable(Int64),
        is_deleted Bool,
        created_at Int64,
        expire_at Int64,
        update_count Int32
    ) ENGINE = MergeTree() ORDER BY id
    ''')
    
    logger.info("ClickHouse schema created successfully.")
    client.disconnect()

# Combined task to read from MongoDB and load into ClickHouse
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
                doc['_id'] = str(doc['_id'])
                batch.append(doc)
            except StopIteration:
                break
        if batch:
            logger.info(f"Batch {batch_number} retrieved from MongoDB: {batch}")
            
            # Insert documents into ClickHouse
            clickhouse_client.execute('''
                INSERT INTO bronze.videos (
                    id, owner_username, owner_id, title, tags, uid, visit_count, owner_name, duration,
                    posted_timestamp, comments, like_count, is_deleted, created_at, expire_at, update_count
                ) VALUES
                    ''', 
                [(doc['id'], doc['owner_username'], doc['owner_id'], doc['title'], doc.get('tags'),
                  doc['uid'], doc['visit_count'], doc['owner_name'], doc['duration'], doc['posted_timestamp'],
                  doc.get('comments'), doc.get('like_count'), doc['is_deleted'], doc['created_at'],
                  doc['expire_at'], doc['update_count']) 
                 for doc in batch]
            )
            logger.info(f"Batch {batch_number} inserted into ClickHouse.")
            batch_number += 1
    
    clickhouse_count = clickhouse_client.execute('SELECT count(*) FROM bronze.videos')
    logger.info(f"Total records in ClickHouse: {clickhouse_count}")
    
    client.close()
    clickhouse_client.disconnect()

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,  # Adjust retries if needed
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
