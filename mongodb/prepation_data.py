import json


def transform_data(input_data):
    # Transform the data according to the desired schema
    transformed_data = {
        "_id": input_data["_id"],
        "object": {
            "id": input_data["object"]["id"]["$numberInt"],
            "owner_username": input_data["object"]["owner_username"],
            "owner_id": input_data["object"]["owner_id"],
            "title": input_data["object"]["title"],
            "tags": input_data["object"]["tags"],
            "uid": input_data["object"]["uid"],
            "visit_count": input_data["object"]["visit_count"]["$numberInt"],
            "owner_name": input_data["object"]["owner_name"],
            "duration": input_data["object"]["duration"]["$numberInt"],
            "posted_date": input_data["object"]["posted_date"],
            "posted_timestamp": input_data["object"]["posted_timestamp"]["$numberInt"],
            "sdate_rss": input_data["object"]["sdate_rss"],
            "sdate_rss_tp": input_data["object"]["sdate_rss_tp"]["$numberInt"],
            "comments": input_data["object"]["comments"],
            "like_count": input_data["object"]["like_count"],
            "description": input_data["object"]["description"],
            "is_deleted": input_data["object"]["is_deleted"]
        },
        "created_at": input_data["created_at"]["$date"]["$numberLong"],
        "expire_at": input_data["expire_at"]["$date"]["$numberLong"],
        "update_count": input_data["update_count"]["$numberInt"]
    }
    return transformed_data


def process_file(path_input_file, path_output_file):
    # Read the input file and process each line
    transformed_data_list = []
    with open(path_input_file, 'r', encoding='utf-8') as input_file:
        for line in input_file:
            try:
                input_data = json.loads(line.strip())
                transformed_data = transform_data(input_data)
                transformed_data_list.append(transformed_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    # Write the transformed data to the output file
    with open(path_output_file, 'w', encoding='utf-8') as output_file:
        for data in transformed_data_list:
            output_file.write(json.dumps(data, ensure_ascii=False) + '\n')


# Use the functions to process the input file and save to the output file
input_file_path = '../db/output.json'  # Replace with the path to your input file
output_file_path = './output.json'  # Replace with the desired path for the output file
process_file(input_file_path, output_file_path)
