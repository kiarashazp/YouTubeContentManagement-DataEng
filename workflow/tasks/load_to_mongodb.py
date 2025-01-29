from airflow.providers.mongo.hooks.mongo import MongoHook
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_mongodb(transformed_data: list[dict], **kwargs) -> None:
    """
    Loads transformed data into MongoDB.

    Args:
        transformed_data (list[dict]): List of dictionaries containing transformed data.
        **kwargs: Additional parameters including 'db_name' and 'collection_name'.
    """
    db_name = kwargs.get('db_name', 'videos')
    collection_name = kwargs.get('collection_name', 'videos')

    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()

    db = client[db_name]
    collection = db[collection_name]

    try:
        collection.insert_many(transformed_data)
        logger.info(f"Successfully inserted {len(transformed_data)} documents into MongoDB")
    except Exception as ve:
        logger.error(f"Error inserting many documents in MongoDB: {ve}")
    finally:
        client.close()
