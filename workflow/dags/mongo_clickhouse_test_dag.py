from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
from airflow.models import Variable
import logging
from bson import ObjectId

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_last_processed_id():
    """Get the last processed MongoDB ObjectId from ClickHouse"""
    client = Client(host='clickhouse', user='airflow', password='airflow')
    result = client.execute('SELECT MAX(mongo_id) FROM bronze.videos')
    client.disconnect()
    return str(result[0][0]) if result[0][0] else None

def process_mongo_batch(mongo_collection, last_id, batch_size):
    """Process a single batch of MongoDB documents"""
    query = {} if last_id is None else {'_id': {'$gt': ObjectId(last_id)}}
    
    return mongo_collection.find(query)\
        .sort('_id', 1)\
        .limit(batch_size)

def stream_to_clickhouse(**kwargs):
    """Stream data from MongoDB to ClickHouse in batches"""
    try:
        batch_size = int(Variable.get("mongo_batch_size", default_var=1000))
        
        # Initialize connections
        mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
        mongo_client = mongo_hook.get_conn()
        mongo_collection = mongo_client['videos']['videos']
        
        clickhouse_client = Client(host='clickhouse', user='airflow', password='airflow')
        
        # Create schema if not exists
        clickhouse_client.execute('CREATE DATABASE IF NOT EXISTS bronze')
        clickhouse_client.execute('''
            CREATE TABLE IF NOT EXISTS bronze.videos (
                mongo_id String,
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
            ) ENGINE = MergeTree()
            ORDER BY (mongo_id, id)
        ''')

        # Get last processed ID
        last_processed_id = get_last_processed_id()
        
        total_processed = 0
        while True:
            # Process batch
            cursor = process_mongo_batch(mongo_collection, last_processed_id, batch_size)
            
            batch_data = []
            last_id = None
            
            for doc in cursor:
                last_id = str(doc['_id'])
                batch_data.append((
                    last_id,
                    doc['id'],
                    doc['owner_username'],
                    doc['owner_id'],
                    doc['title'],
                    doc['tags'],
                    doc['uid'],
                    doc['visit_count'],
                    doc['owner_name'],
                    doc['duration'],
                    doc['posted_timestamp'],
                    doc['comments'],
                    doc['like_count'],
                    doc['is_deleted'],
                    doc['created_at'],
                    doc['expire_at'],
                    doc['update_count']
                ))
            
            # If no documents were processed, we're done
            if not batch_data:
                break
                
            # Insert batch into ClickHouse
            clickhouse_client.execute('''
                INSERT INTO bronze.videos (
                    mongo_id, id, owner_username, owner_id, title, tags, uid, 
                    visit_count, owner_name, duration, posted_timestamp, comments,
                    like_count, is_deleted, created_at, expire_at, update_count
                ) VALUES
            ''', batch_data)
            
            total_processed += len(batch_data)
            last_processed_id = last_id
            
            logger.info(f"Processed {total_processed} documents. Last ID: {last_processed_id}")
            
        logger.info(f"Completed processing {total_processed} documents")
        
        # Clean up connections
        mongo_client.close()
        clickhouse_client.disconnect()

    except Exception as error:
        logger.error(f"Error processing data: {error}")
        raise

# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

with DAG('mongo_clickhouse_streaming_dag', 
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    stream_task = PythonOperator(
        task_id='stream_mongo_to_clickhouse',
        python_callable=stream_to_clickhouse,
        provide_context=True,
    )

stream_task
