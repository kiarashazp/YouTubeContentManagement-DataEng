import json
from datetime import datetime


def transform_data(input_data):
    # Transform the data according to the desired schema
    try:
        transformed_data = {
            "_id": input_data["_id"],
            "object": {
                "id": str(input_data["object"]["id"]["$numberInt"]),
                "owner_username": str(input_data["object"]["owner_username"]),
                "owner_id": str(input_data["object"]["owner_id"]),
                "title": str(input_data["object"]["title"]),
                "tags": str(input_data["object"]["tags"]) if input_data["object"]["tags"] is not None else None,
                "uid": str(input_data["object"]["uid"]),
                "visit_count": int(input_data["object"]["visit_count"]["$numberInt"]),
                "owner_name": str(input_data["object"]["owner_name"]),
                "duration": int(input_data["object"]["duration"]["$numberInt"]),
                "posted_date": str(input_data["object"]["posted_date"]),  # Assuming this is already a string
                "posted_timestamp": datetime.fromtimestamp(int(input_data["object"]["posted_timestamp"]["$numberInt"])).isoformat(),
                "sdate_rss": str(input_data["object"]["sdate_rss"]),  # Assuming this is already a string
                "sdate_rss_tp": datetime.fromtimestamp(int(input_data["object"]["sdate_rss_tp"]["$numberInt"])).isoformat(),
                "comments": str(input_data["object"]["comments"]) if input_data["object"]["comments"] is not None else None,
                "like_count": int(input_data["object"]["like_count"]) if input_data["object"]["like_count"] is not None else None,
                "description": str(input_data["object"]["description"]) if input_data["object"]["description"] is not None else None,
                "is_deleted": bool(input_data["object"]["is_deleted"])
            },
            "created_at": int(input_data["created_at"]["$date"]["$numberLong"]),
            "expire_at": int(input_data["expire_at"]["$date"]["$numberLong"]),
            "update_count": int(input_data["update_count"]["$numberInt"])
        }
    except Exception as ve:
        return {"Error": f"data with id {input_data['_id']} has exception : {ve}"}
    return transformed_data


def process_file(path_input_file, path_output_file):
    # Read the input file and process each line
    with open(path_output_file, 'w', encoding='utf-8') as output_file:
        with open(path_input_file, 'r') as input_file:
            # Read 6328629 % 10000 of first json file
            x = 6328629 % 10000
            lines = [input_file.readline() for _ in range(x)]
            for line in lines:
                try:
                    input_data = json.loads(line.strip())
                    transformed_data = transform_data(input_data)
                    output_file.write(json.dumps(transformed_data, ensure_ascii=False) + '\n')
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

            while True:
                lines = [input_file.readline() for _ in range(10000)]
                # Break if we've reached the end of the file
                if not any(lines):
                    break
                for line in lines:
                    try:
                        input_data = json.loads(line.strip())
                        transformed_data = transform_data(input_data)
                        output_file.write(json.dumps(transformed_data, ensure_ascii=False) + '\n')
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")

