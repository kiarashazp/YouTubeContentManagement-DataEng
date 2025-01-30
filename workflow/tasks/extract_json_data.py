import json
import logging
import tempfile

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

