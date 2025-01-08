from config import MONGO_CONN_ID
from airflow.hooks.mongo_hook import MongoHook
from airflow.models import Variable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_mongo_data():
    try:
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        collection_name = Variable.get("mongo_collection_name")
        collection = mongo_hook.get_collection(collection_name)
        data = collection.find()
        
        for document in data:
            logger.info(document)
        
    except Exception as error:
        logger.error(f"Error extracting data from MongoDB: {error}")
        raise


if __name__ == "__main__":
    extract_mongo_data()
