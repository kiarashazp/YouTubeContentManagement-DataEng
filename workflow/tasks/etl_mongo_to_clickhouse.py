import logging
from datetime import timedelta, datetime
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from pendulum import Timezone
from utils.load_sql_query import load_query_from_file
from clickhouse_driver import Client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def etl_mongo_to_clickhouse(**kwargs):
    """
    Extracts data from MongoDB, transforms it, and loads it into ClickHouse.
    """
    try:
        # Extraction part
        batch_size = int(Variable.get("batch_size", default_var=1000))
        ti = kwargs['ti']
        start_date = datetime(2025, 1, 21, 17, 1, 38, 298788, tzinfo=Timezone('UTC'))
        end_data = datetime(2025, 1, 31, 17, 1, 38, 298788, tzinfo=Timezone('UTC'))
        logger.info(f"Extracting data for date: {start_date}")

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
                    "$gte": 1734715367322,
                    "$lt": 1735913230017
                }
            }
            logger.info(f"Executing MongoDB query: {mongo_query}")

            cursor = collection.find(mongo_query).batch_size(batch_size)
            
            while cursor.alive:
                batch_data = []
                try:
                    for _ in range(batch_size):
                        doc = cursor.next()
                        batch_data.append(doc)
                except StopIteration:
                    break
                
                if not batch_data:
                    break

                # Transformation part
                logger.info(f"Transforming batch of {len(batch_data)} documents")
                transformed_batch = []
                for doc in batch_data:
                    videos_values = {
                        'id': str(doc.get('id', '')),
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


