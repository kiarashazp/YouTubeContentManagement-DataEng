import logging
from datetime import timedelta
from airflow.models.dagrun import DagRun


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_mongo_data(**kwargs):
    """
    Extracts data from MongoDB for a specific date range.

    Args:
        **kwargs: Airflow context containing 'logical_date', 'db_name', and 'collection_name'.

    Returns:
        list: A list of documents retrieved from MongoDB.
    """
    try:
        logical_date = DagRun.execution_date  # Get the logical date (execution date) for the current run
        # logical_date = kwargs['logical_date']
        start_date = logical_date  # Start of the interval
        end_date = logical_date + timedelta(days=1)  # End of the interval

        logger.info(f"Extracting data for date range: {start_date} to {end_date}")

        # Connect to MongoDB
        mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
        client = mongo_hook.get_conn()

        # Get database and collection names (with defaults)
        db_name = kwargs.get('db_name', 'videos')
        collection_name = kwargs.get('collection_name', 'videos')
        logger.info(f"Using database: {db_name}, collection: {collection_name}")

        db = client[db_name]
        collection = db[collection_name]

        # Query MongoDB for data within the date range
        mongo_query = {
            "created_at": {
                "$gte": start_date.isoformat(),
                "$lt": end_date.isoformat(),
            }
        }

        logger.info(f"Executing MongoDB query: {mongo_query}")
        mongo_data = list(collection.find(mongo_query))

        logger.info(f"Retrieved {len(mongo_data)} documents from MongoDB")
        return mongo_data

    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise
