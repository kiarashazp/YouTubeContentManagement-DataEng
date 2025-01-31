import json
import logging
from datetime import datetime

from airflow.providers.mongo.hooks.mongo import MongoHook

from utils import connect_to_s3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 1000  # Define batch size


def etl_json_to_mongodb(**kwargs):
    """Extracts JSON files from S3, transforms the data, and loads it into MongoDB."""

    s3_resource, bucket_name, response = connect_to_s3.connected_to_s3()

    if 'Contents' not in response:
        logger.warning("No files found in the S3 bucket.")
        return []

    # Process each file in the S3 bucket
    for file in response['Contents']:
        if not file['Key'].endswith(".json"):
            continue

        obj = s3_resource.get_object(Bucket=bucket_name, Key=file['Key'])
        content = obj['Body'].read().decode('utf-8')

        batch = []
        for line in content.splitlines():
            try:
                json_data = json.loads(line)
                transformed_json = transforming_data(json_data)

                if transformed_json:
                    batch.append(transformed_json)

                if len(batch) >= BATCH_SIZE:
                    load_to_mongodb(transformed_json, batch_data=batch)
                    batch.clear()

            except json.JSONDecodeError as jde:
                logger.error(f"Error decoding JSON content in file {file['Key']}: {jde}")


def transforming_data(input_data):
    """Transforms input data into the required format for MongoDB."""
    try:
        obj = input_data.get("object", {})

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
            "expire_at": int(datetime.fromisoformat(input_data.get("expire_at", '1970-01-01')).timestamp()),
            "update_count": int(input_data.get("update_count", 0))
        }

        return transformed_doc

    except Exception as ve:
        logger.error(f"Error transforming document with _id {input_data.get('_id')} in ETL s3 to MongoDB: {ve}")


def connect_to_mongo(db_name='videos', collection_name='videos'):
    """Establishes a connection to MongoDB and returns the collection object."""

    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()
    db = client[db_name]
    return client, db[collection_name]


def load_to_mongodb(data, batch_data=None) -> None:
    """Loads transformed data into MongoDB."""

    client, collection = connect_to_mongo()
    try:
        if batch_data:
            collection.insert_many(batch_data)
        else:
            collection.insert_one(data)
        logger.info(f"Successfully inserted document into MongoDB: {data['_id']}")
    except Exception as ve:
        logger.error(f"Error inserting many documents in MongoDB: {ve}")
    finally:
        client.close()
