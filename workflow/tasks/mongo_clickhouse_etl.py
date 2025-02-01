import datetime

from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from clickhouse_driver import Client
import logging

from utils.load_sql_query import load_query_from_file
from utils.prepare_batch_data import prepare_batch_data


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def mongo_clickhouse_etl(**kwargs):
    # Step 1: Initialize variables and connections
    batch_size = int(Variable.get("mongo_batch_size", default_var=1000))

    try:
         # Connect to MongoDB
        mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
        with mongo_hook.get_conn() as client:
            db_name = kwargs.get('db_name', 'videos')
            collection_name = kwargs.get('collection_name', 'videos')
            logger.info(f"Using database: {db_name}, collection: {collection_name}")
            db = client[db_name]
            collection = db[collection_name]
            cursor = collection.find().batch_size(batch_size)
             # Initialize counters and track processed documents
            batch_number = 0
            processed_ids = set()

            # Process documents in batches
            while cursor.alive:
                batch = []
                for _ in range(batch_size):
                    try:
                        # Fetch the next document from MongoDB
                        doc = cursor.next()

                        # Extract the object data
                        obj_data = doc.get('object', {})

                        # Check if the document has already been processed
                        if str(doc['_id']) not in processed_ids:
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
                                'created_at': obj_data.get('created_at', datetime.datetime.now()),
                                'expire_at': obj_data.get('expire_at', datetime.datetime.now()),
                                'update_count': obj_data.get('update_count', 0)
                            }
                            batch.append(processed_doc)
                            processed_ids.add(str(doc['_id']))
                    except StopIteration:
                        # Handle the final batch (if any)
                        logger.info("Reached the end of the MongoDB cursor.")

                # Insert the batch into ClickHouse
                # Process the batch (even if it's not a full batch)
                if batch:
                    # Prepare batch data for insertion
                    batch_data = prepare_batch_data(batch)
                    # Load the INSERT query from file
                    query_file_path = "utils/insert_videos_query.sql"
                    insert_query = load_query_from_file(query_file_path)

                    try:
                        # Insert documents into ClickHouse using the loaded query
                        with Client(host='clickhouse', user='airflow', password='airflow') as clickhouse_client:
                            clickhouse_client.execute(insert_query, batch_data)
                            logger.info(f"Batch {batch_number} inserted into ClickHouse.")
                            batch_number += 1
                    except Exception as ve:
                        logger.error(
                            f"Failed to insert batch {batch_number} into ClickHouse. Error: {ve}\n"
                            f"Batch size: {len(batch)}\n"
                            f"First document ID: {batch[0]['id'] if batch else 'N/A'}"
                        )
                    clickhouse_count = clickhouse_client.execute('SELECT count(*) FROM bronze.videos')
                    logger.info(f"Total records in ClickHouse: {clickhouse_count}")
                else:
                    # No more documents to process
                    break
        logger.info("ETL task completed")
    except Exception as e:    
        logger.error(f"ETL task failed: {e}")
        raise
    

    