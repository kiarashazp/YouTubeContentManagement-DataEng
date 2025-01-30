import json
import logging
import tempfile

from datetime import timedelta, datetime
from airflow.models.dagrun import DagRun
from airflow.providers.mongo.hooks.mongo import MongoHook

from utils import connect_to_s3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_json_data(**kwargs) -> list[dict]:
    """
    Extracts JSON data from files in an S3 bucket.

    Args:
        **kwargs: Additional parameters.

    Returns:
        list[dict]: A list of dictionaries containing JSON data.

    Raises:
        Exception: If an error occurs during the extraction process.
    """
    try:
        start_date = DagRun.execution_date  # Start of the interval
        end_date = start_date + timedelta(days=1)  # End of the interval
        logger.info(f"Extracting data for date range: {start_date} to {end_date}")

        # Connect to S3
        s3_resource, bucket_name, response = connect_to_s3.connected_to_s3()
        json_data_list = []

        if 'Contents' not in response:
            logger.warning("No files found in the S3 bucket.")
            return json_data_list

        # Process each file in the S3 bucket
        for file in response['Contents']:
            if not file['Key'].endswith(".json"):
                continue  # Skip non-JSON files

            logger.info(f"Processing JSON file: {file['Key']}")

            try:
                # Read the file content
                obj = s3_resource.get_object(Bucket=bucket_name, Key=file['Key'])
                content = obj['Body'].read().decode('utf-8')

                # Parse JSON content line by line
                for line in content.splitlines():
                    try:
                        json_data = json.loads(line)
                        json_data_list.append(json_data)
                        logger.info(f"load json files")
                    except json.JSONDecodeError as jde:
                        logger.error(f"Error decoding JSON content in file {file['Key']}: {jde}")

            except Exception as e:
                logger.error(f"Error reading file {file['Key']} from S3: {e}")
                continue  # Skip to the next file

        logger.info(f"Extracted {len(json_data_list)} JSON documents from S3")
        # Save the JSON data to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as tmp_file:
            json.dump(json_data_list, tmp_file)
            tmp_filename = tmp_file.name
            logger.info(f"Data written to temporary file: {tmp_filename}")

        return tmp_filename

    except Exception as e:
        logger.error(f"Failed to extract JSON data from S3: {e}")
        raise  # Re-raise the exception to ensure the task fails


def extract_from_s3_and_transform_data(**kwargs):
    s3_resource, bucket_name, response = connect_to_s3.connected_to_s3()

    if 'Contents' not in response:
        logger.warning("No files found in the S3 bucket.")
        return []

    # Process each file in the S3 bucket
    for file in response['Contents']:
        if not file['Key'].endswith(".json"):
            continue

        logger.info(f"Processing JSON file: {file['Key']}")
        obj = s3_resource.get_object(Bucket=bucket_name, Key=file['Key'])
        content = obj['Body'].read().decode('utf-8')

        for line in content.splitlines():
            try:
                json_data = json.loads(line)
                logger.info(f"load json files")
                transformed_json = transforming_data(json_data)
                logger.info(f"transformed data")
                load_to_mongodb(transformed_json)
                logger.info(f"insert in file ")
            except json.JSONDecodeError as jde:
                logger.error(f"Error decoding JSON content in file {file['Key']}: {jde}")


def transforming_data(input_data):
    try:
        obj = input_data["object"]  # Extract object data

        transformed_doc = {
            "_id": input_data["_id"],
            "object": {
                "id": obj["id"],
                "owner_username": str(obj.get("owner_username", '')),
                "owner_id": str(obj.get("owner_id", '')),
                "title": str(obj.get("title", '')),
                "tags": str(obj.get("tags", '')),
                "uid": str(obj.get("uid", '')),
                "visit_count": obj.get("visit_count", 0),
                "owner_name": str(obj.get("owner_name", '')),
                "duration": obj.get("duration", 0),
                "posted_date": str(obj.get("posted_date", '1970-01-01')),
                "posted_timestamp": datetime.fromtimestamp(int(obj.get("posted_timestamp", 0))).isoformat(),
                "comments": str(obj.get("comments", '')),
                "like_count": obj.get("like_count", None),
                "description": str(obj.get("description", '')),
                "is_deleted": bool(obj.get("is_deleted", False))
            },
            "created_at": int(datetime.fromisoformat(input_data.get("created_at", '1970-01-01')).timestamp()),
            # Convert ISO to timestamp
            "expire_at": int(datetime.fromisoformat(input_data.get("expire_at", '1970-01-01')).timestamp()),
            # Convert ISO to timestamp
            "update_count": int(input_data.get("update_count", 0))
        }

        return transformed_doc

    except Exception as ve:
        logger.error(f"Error transforming document with _id {input_data.get('_id')} in ETL s3 to MongoDB: {ve}")


def connect_to_mongo(**kwargs):
    db_name = kwargs.get('db_name', 'videos')
    collection_name = kwargs.get('collection_name', 'videos')

    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()

    db = client[db_name]
    collection = db[collection_name]

    return client, collection


def load_to_mongodb(data, **kwargs) -> None:
    """
    Loads transformed data from a file into MongoDB.

    Args:
        data (json): a transformed data for insert in mongodb.
        **kwargs: Additional parameters including 'db_name' and 'collection_name'.
    """
    client, collection = connect_to_mongo()

    try:
        # Insert transformed data into MongoDB
        collection.insert_one(data)
        logger.info(f"Successfully inserted {data} documents into MongoDB")
    except Exception as ve:
        logger.error(f"Error inserting many documents in MongoDB: {ve}")
    finally:
        client.close()
