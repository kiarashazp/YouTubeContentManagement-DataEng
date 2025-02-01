import json
import logging
from datetime import datetime

from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

from utils import utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def etl_json_to_mongodb(**kwargs):
    """Extracts JSON files from S3, transforms the data, and loads it into MongoDB."""

    BATCH_SIZE = int(Variable.get("batch_size", default_var=10000))
    start_date = kwargs['start_date']

    list_file_modified = utils.get_new_files(start_date, "json")
    s3_resource, bucket_name = utils.connected_to_s3()

    logger.info(f"------ len list file modified is: {len(list_file_modified)}")
    for file in list_file_modified:
        obj = s3_resource.Object(Bucket=bucket_name, key=file)
        content = obj['Body'].read().decode('utf-8')
        batch = []
        for line in content.splitlines():
            try:
                json_data = json.loads(line)
                transformed_json = transform_json_data(json_data)
                if transformed_json:
                    batch.append(transformed_json)

                if BATCH_SIZE <= len(batch):
                    load_json_to_mongodb(transformed_json, batch_data=batch, **kwargs)
                    batch.clear()

            except json.JSONDecodeError as jde:
                logger.error(f"Error decoding JSON content in file {file['Key']}: {jde}")


def transform_json_data(input_data):
    """Transforms input data into the required format for MongoDB."""
    try:
        obj = input_data.get("object", {})

        comments = obj.get("comments", '')
        count_comments = 0 if comments is None else comments.split(' - ')

        transformed_doc = {
            "_id": input_data["_id"],
            "object": {
                "id": obj["id"],
                "owner_username": str(obj.get("owner_username", '')),
                "owner_id": str(obj.get("owner_id", '')),
                "title": str(obj.get("title", '')),
                "tags": str(obj.get("tags", '')),
                "uid": str(obj.get("uid", '')),
                "visit_count": int(obj.get("visit_count", 0)),
                "owner_name": str(obj.get("owner_name", '')),
                "duration": int(obj.get("duration", 0)),
                "posted_date": str(obj.get("posted_date", '1970-01-01')),
                "posted_timestamp": datetime.fromtimestamp(int(obj.get("posted_timestamp", 0))).isoformat(),
                "comments": count_comments,
                "like_count": obj.get("like_count", None),
                "description": str(obj.get("description", '')),
                "is_deleted": bool(obj.get("is_deleted", False))
            },
            "created_at": datetime.isoformat(input_data.get("created_at", '1970-01-01')),
            "expire_at": datetime.isoformat(input_data.get("expire_at", '1970-01-01')),
            "update_count": int(input_data.get("update_count", 0))
        }

        return transformed_doc

    except Exception as ve:
        logger.error(f"Error transforming document with _id {input_data.get('_id')} in ETL s3 to MongoDB: {ve}")


def connect_to_mongo(**kwargs):
    """Establishes a connection to MongoDB and returns the collection object."""

    db_name = kwargs.get('db_name', 'videos')
    collection_name = kwargs.get('collection_name', 'videos')

    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()
    db = client[db_name]
    return client, db[collection_name]


def load_json_to_mongodb(data, batch_data=None, **kwargs) -> None:
    """Loads transformed data into MongoDB."""
    client, collection = connect_to_mongo(**kwargs)
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
