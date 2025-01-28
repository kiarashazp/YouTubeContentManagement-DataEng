import json
from workflow.utils import connect_to_s3
from datetime import datetime
from airflow.providers.mongo.hooks.mongo import MongoHook


def extract_json_from_s3():
    s3_resource, bucket_name, response = connect_to_s3.connected_to_s3()
    json_datas = list()
    if 'Contents' in response:
        for file in response['Contents']:
            if file['Key'].endswith(".json"):
                obj = s3_resource.get_object(Bucket=bucket_name, Key=file['Key'])
                content = obj['Body'].read().decode('utf-8')
                for line in content.splitlines():
                    json_data = json.loads(line)
                    json_datas.append(json_data)
    return json_datas


def transform_json_file(input_datas):
    transformed_datas = list()

    for input_data in input_datas:
        try:
            transformed_data = {
                "_id": input_data["_id"],
                "object": {
                    "id": input_data["object"]["id"],
                    "owner_username": str(input_data["object"]["owner_username"]),
                    "owner_id": str(input_data["object"]["owner_id"]),
                    "title": str(input_data["object"]["title"]),
                    "tags": str(input_data["object"]["tags"]) if input_data["object"]["tags"] is not None else None,
                    "uid": str(input_data["object"]["uid"]),
                    "visit_count": input_data["object"]["visit_count"],
                    "owner_name": str(input_data["object"]["owner_name"]),
                    "duration": input_data["object"]["duration"],
                    "posted_date": str(input_data["object"]["posted_date"]),
                    "posted_timestamp": datetime.fromtimestamp(int(input_data["object"]["posted_timestamp"])).isoformat(),
                    "sdate_rss": str(input_data["object"]["sdate_rss"]),
                    "sdate_rss_tp": datetime.fromtimestamp(int(input_data["object"]["sdate_rss_tp"])).isoformat(),
                    "comments": str(input_data["object"]["comments"]) if input_data["object"]["comments"] is not None else None,
                    "like_count": input_data["object"]["like_count"] if input_data["object"]["like_count"] is not None else None,
                    "description": str(input_data["object"]["description"]) if input_data["object"]["description"] is not None else None,
                    "is_deleted": bool(input_data["object"]["is_deleted"])
                },
                "created_at": int(datetime.fromisoformat(input_data["created_at"]).timestamp()),  # Convert ISO to timestamp
                "expire_at": int(datetime.fromisoformat(input_data["expire_at"]).timestamp()),  # Convert ISO to timestamp
                "update_count": int(input_data["update_count"])
            }
            transformed_datas.append(transformed_data)

        except Exception as ve:
            logger.info(msg=f"I get error when transformed data in ETL S3ToMongo. error this: {ve}")

    return transformed_datas


def load_to_mongo_db(**kwargs):
    db_name = kwargs.get('db_name', 'videos')
    collection_name = kwargs.get('collection_name', 'videos')

    mongo_hook = MongoHook(conn_id='MONGO_CONN_ID')
    client = mongo_hook.get_conn()

    db = client[db_name]
    collection = db[collection_name]

    transformed_datas = kwargs.get('transformed_datas', list())

    try:
        collection.insert_many(transformed_datas)
    except Exception as ve:
        logger.info(msg=f"I get error when insert many in mongo. error this: {ve}")
    finally:
        client.close()
