import json
from datetime import datetime


def transform_json_data(input_data):
    """Transforms input data into the required format for MongoDB."""

    def extract_number(value):
        """Helper function to extract numbers from BSON format."""
        if isinstance(value, dict):
            if "$numberInt" in value:
                return int(value["$numberInt"])
            elif "$numberLong" in value:
                return int(value["$numberLong"])
            elif "$date" in value:
                return int(value["$date"]["$numberLong"])  # Handle the $date field correctly
        return value or 0

    try:
        obj = input_data.get("object", {})

        # Extract comments if present, otherwise default to 0
        comments = obj.get("comments", None)
        count_comments = 0 if comments is None else len(comments.split(' - ')) if comments else 0

        # Process the transformed document
        transformed_doc = {
            "_id": input_data["_id"],
            "object": {
                "id": extract_number(obj.get("id", {})),
                "owner_username": str(obj.get("owner_username", '')),
                "owner_id": str(obj.get("owner_id", '')),
                "title": str(obj.get("title", '')),
                "tags": str(obj.get("tags", 'None') or 'None'),
                "uid": str(obj.get("uid", '')),
                "visit_count": extract_number(obj.get("visit_count", 0)),
                "owner_name": str(obj.get("owner_name", '')),
                "duration": extract_number(obj.get("duration", 0)),
                "posted_date": str(obj.get("posted_date", '1970-01-01 00:00:00')),
                "posted_timestamp": datetime.fromtimestamp(extract_number(obj.get("posted_timestamp", 0))).isoformat(),
                "comments": count_comments,
                "like_count": obj.get("like_count", None),  # if it's null, it stays null
                "description": str(obj.get("description", '')),
                "is_deleted": bool(obj.get("is_deleted", False))
            },
            "created_at": datetime.fromtimestamp(extract_number(input_data.get("created_at", {})) / 1000).isoformat(),
            "expire_at": datetime.fromtimestamp(extract_number(input_data.get("expire_at", {})) / 1000).isoformat(),
            "update_count": extract_number(input_data.get("update_count", 0))
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
                        transformed_data = transform_json_data(input_data)
                        output_file.write(json.dumps(transformed_data, ensure_ascii=False) + '\n')
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")


process_file("videos.json", "rep_videos.json")
