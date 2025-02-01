import logging
from datetime import timedelta, datetime

import pytz
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from pendulum import Timezone
from utils.load_sql_query import load_query_from_file
from clickhouse_driver import Client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_convert_datetime(value):
    """
    Convert a value to datetime.
    - If already a datetime, return as-is.
    - If string in ISO format, convert using `fromisoformat()`.
    - If string in MongoDB format (with 'T' separator), use `strptime()`.
    """
    if isinstance(value, datetime):
        return value  # Already a datetime object

    elif isinstance(value, str):
        try:
            # If string format is "YYYY-MM-DD HH:MM:SS", convert it
            if "T" in value:
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)  # Handle invalid format gracefully

    return datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)


def etl_mongo_to_clickhouse(**kwargs):
    """
    Extracts data from MongoDB, transforms it, and loads it into ClickHouse.
    """
    try:
        # Extraction part
        batch_size = int(Variable.get("batch_size", default_var=1000))
        start_date = kwargs['start_date']

        logger.info(f"Extracting data for date gte: {start_date}")

        # Connect to MongoDB
        mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
        with mongo_hook.get_conn() as client:
            db_name = kwargs.get('db_name', 'videos')
            collection_name = kwargs.get('collection_name', 'videos')
            logger.info(f"Using database: {db_name}, collection: {collection_name}")
            db = client[db_name]
            collection = db[collection_name]
            
            # Query MongoDB
            mongo_query = {
                "created_at": {
                    "$gte": start_date,
                }
            }
            logger.info(f"Executing MongoDB query: {mongo_query}")

            cursor = collection.find(mongo_query).batch_size(batch_size)
            total_extracted = 0
            while cursor.alive:
                batch_data = []
                try:
                    for _ in range(batch_size):
                        doc = cursor.next()
                        batch_data.append(doc)
                except StopIteration:
                    # Handle the final batch (if any)
                    if batch_data:
                        logger.info(f"Extracted final batch of {len(batch_data)} documents")
                    else:
                        logger.info("No more documents to extract.")
                        break  # Exit the inner loop

                total_extracted += len(batch_data)
                logger.info(f"Extracted batch of {len(batch_data)} documents, total extracted: {total_extracted}")
                
                # Transformation part
                logger.info(f"Transforming batch of {len(batch_data)} documents")
                transformed_batch = []
                created_at = doc.get('created_at') if doc.get('created_at') is not None else '1970-01-01T00:00:00'
                expired_at = doc.get('expired_at') if doc.get('expired_at') is not None else '1970-01-01T00:00:00'

                for doc in batch_data:
                    obj_data = doc.get('object', {})
                    videos_values = {
                        'id': str(doc.get('id', '')),
                        'owner_username': obj_data.get('owner_username', ''),
                        'owner_id': obj_data.get('owner_id', ''),
                        'title': obj_data.get('title', ''),
                        'tags': obj_data.get('tags', ''),
                        'uid': obj_data.get('uid', ''),
                        'visit_count': obj_data.get('visit_count', 0),
                        'owner_name': obj_data.get('owner_name', ''),
                        'duration': obj_data.get('duration', 0),
                        'comments': obj_data.get('comments', 0),
                        'like_count': obj_data.get('like_count', 0),
                        'is_deleted': obj_data.get('is_deleted', False),
                        'created_at': safe_convert_datetime(created_at),
                        'expire_at': safe_convert_datetime(expired_at),
                        'update_count': doc.get('update_count', 0)
                    }
                    transformed_batch.append(videos_values)

                logger.info(f"Successfully transformed batch of {len(transformed_batch)} documents")

                # Load part
                query_file_path = "utils/insert_videos_query.sql"
                insert_query = load_query_from_file(query_file_path)
                if not insert_query:
                    logger.error("No SQL query loaded. Aborting.")
                    return

                if not transformed_batch:
                    logger.info("No data to load into ClickHouse.")
                    return

                with Client(host='clickhouse', user='airflow', password='airflow') as ch_client:
                    ch_client.execute(insert_query, transformed_batch)
                    logger.info(f"Successfully inserted batch of {len(transformed_batch)} rows into bronze.videos.")
        
        logger.info("ETL task completed")

    except Exception as e:
        logger.error(f"ETL task failed: {e}")
        raise


