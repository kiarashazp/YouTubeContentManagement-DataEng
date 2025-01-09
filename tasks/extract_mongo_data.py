from config import MONGO_CONN_ID
from airflow.hooks.mongo_hook import MongoHook
from airflow.models import Variable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_mongo_data(**kwargs):
    try:
        # You can adjust the batch size in the Airflow UI without changing the code.
        batch_size = int(Variable.get("mongo_batch_size", default_var=1000))
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        collection_name = Variable.get("mongo_collection_name")
        collection = mongo_hook.get_collection(collection_name)
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

if __name__ == "__main__":
    extract_mongo_data()

