from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from clickhouse_driver import Client
from airflow.models import Variable

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Task to read from MongoDB
def read_from_mongo(**kwargs):
    try:
        # You can adjust the batch size in the Airflow UI without changing the code.
        batch_size = int(Variable.get("mongo_batch_size", default_var=1000))
        mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
        client = mongo_hook.get_conn()
        db = client['videos']
        collection = db['videos']
        cursor = collection.find().batch_size(batch_size)
        
        batches = []
        while True:
            batch = list(cursor.next_batch())
            if not batch:
                break
            batches.append(batch)
        
        # Push the batches to XCom
        kwargs['ti'].xcom_push(key='mongo_batches', value=batches)
        
    except Exception as error:
        logger.error(f"Error extracting data from MongoDB: {error}")
        raise


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
    update_count Int32,
        ) ENGINE = MergeTree() ORDER BY name
   ''')
    
    # Close the connection
    client.disconnect()


# Task to load data into ClickHouse
def load_to_clickhouse(**kwargs):
    client = Client(host='clickhouse', user='airflow', password='airflow')
    mongo_batches = kwargs['ti'].xcom_pull(task_ids='read_from_mongo', key='mongo_batches')
    if not mongo_batches:
        raise ValueError("No data batches received from MongoDB extraction task.")
    
    # Insert documents into ClickHouse 
    # Insert documents into ClickHouse
    for batch in mongo_batches:
        client.execute('''
            INSERT INTO bronze.videos (
                    id, owner_username, owner_id, title, tags, uid, visit_count, owner_name, duration,
                    posted_timestamp, comments, like_count, is_deleted, created_at, expire_at, update_count) VALUES 
                    ''',
                       [(doc['id'], doc['owner_username'], doc['owner_id'], doc['title'], doc['tags'], doc['uid'],
                         doc['visit_count'], doc['owner_name'], doc['duration'], doc['posted_timestamp'],
                         doc['comments'],
                         doc['like_count'], doc['is_deleted'], doc['created_at'], doc['expire_at'], doc['update_count'])
                        for doc in batch]
                       )
      
    # Query from ClickHouse to verify the insertion
    clickhouse_result = client.execute('SELECT count(*) FROM bronze.videos')
    print("ClickHouse Result:", clickhouse_result)
    
    # Close the connection
    client.disconnect()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('mongo_clickhouse_example_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    read_task = PythonOperator(
        task_id='read_from_mongo',
        python_callable=read_from_mongo,
        provide_context=True,
    )

    create_schema_task = PythonOperator(
        task_id='create_clickhouse_schema',
        python_callable=create_clickhouse_schema,
    )

    load_task = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse,
        provide_context=True,
    )

read_task >> create_schema_task >> load_task
