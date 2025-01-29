import json
import logging
import tempfile
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_json_data(file_path: str, **kwargs) -> str:
    """
    Transforms a list of JSON data from a file into a structured format and saves it to another temporary file.

    Args:
        file_path (str): The path to the file containing JSON data.
        **kwargs: Additional parameters that can be passed to the function.

    Returns:
        str: The path to the temporary file containing the transformed data.
    """
    try:
        # Read JSON data from the file
        with open(file_path, 'r') as file:
            json_data_list = json.load(file)

        transformed_data = []

        for input_data in json_data_list:
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
                    "created_at": int(datetime.fromisoformat(input_data.get("created_at", '1970-01-01')).timestamp()),  # Convert ISO to timestamp
                    "expire_at": int(datetime.fromisoformat(input_data.get("expire_at", '1970-01-01')).timestamp()),  # Convert ISO to timestamp
                    "update_count": int(input_data.get("update_count", 0))
                }
                transformed_data.append(transformed_doc)

            except Exception as ve:
                logger.error(f"Error transforming document with _id {input_data.get('_id')} in ETL s3 to MongoDB: {ve}")

        # Save the transformed data to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as tmp_file:
            json.dump(transformed_data, tmp_file)
            tmp_filename = tmp_file.name
            logger.info(f"Transformed data written to temporary file: {tmp_filename}")

        return tmp_filename

    except Exception as e:
        logger.error(f"Failed to transform JSON data: {e}")
        raise  # Re-raise the exception to ensure the task fails


    

