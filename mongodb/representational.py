import json
from datetime import datetime

import pytz


def transforming_data(input_data):
    # Transform the data according to the desired schema

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
                "visit_count": int(obj.get("visit_count", 0) or 0),
                "owner_name": str(obj.get("owner_name", '') or ''),
                "duration": int(obj.get("duration", 0) or 0),
                "posted_date": str(obj.get("posted_date", '1970-01-01')),
                "posted_timestamp": datetime.fromtimestamp(int(obj.get("posted_timestamp", 0))).isoformat(),
                "comments": count_comments,
                "like_count": obj.get("like_count", 0) or 0,
                "description": str(obj.get("description", '')),
                "is_deleted": bool(obj.get("is_deleted", False))
            },
            "created_at": (input_data.get("created_at") or datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)),
            "expire_at": (input_data.get("expire_at")),
            "update_count": int(input_data.get("update_count", 0) or 0)
        }

        return transformed_doc

    except Exception as ve:
        print(f"Error transforming document with _id {input_data.get('_id')} -----> {ve}")


def process_file(path_input_file, path_output_file):
    # Read the input file and process each line
    with open(path_output_file, 'w', encoding='utf-8') as output_file:
        with open(path_input_file, 'r') as input_file:
            for line in input_file:
                if line.strip():
                    try:
                        input_data = json.loads(line.strip())
                        transformed_data = transforming_data(input_data)
                        output_file.write(json.dumps(transformed_data) + '\n')
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")


process_file("videos.json", "rep_videos.json")
