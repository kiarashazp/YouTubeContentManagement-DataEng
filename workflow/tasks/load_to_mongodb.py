from airflow.providers.mongo.hooks.mongo import MongoHook
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_mongodb(file_path: str, **kwargs) -> None:
    """
    Loads transformed data from a file into MongoDB.

    Args:
        file_path (str): The path to the file containing transformed data.
        **kwargs: Additional parameters including 'db_name' and 'collection_name'.
    """
    db_name = kwargs.get('db_name', 'videos')
    collection_name = kwargs.get('collection_name', 'videos')

    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()

    db = client[db_name]
    collection = db[collection_name]

    try:
        # Read transformed data from the file
        with open(file_path, 'r') as file:
            transformed_data = json.load(file)
            logger.info(f"Read {len(transformed_data)} transformed documents from file: {file_path}")

        # Insert transformed data into MongoDB
        collection.insert_many(transformed_data)
        logger.info(f"Successfully inserted {len(transformed_data)} documents into MongoDB")
    except Exception as ve:
        logger.error(f"Error inserting many documents in MongoDB: {ve}")
    finally:
        client.close()

