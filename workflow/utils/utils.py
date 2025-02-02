import logging

import boto3
from datetime import datetime, timedelta
import pytz


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connected_to_s3():
    endpoint_url = 'https://s3.ir-thr-at1.arvanstorage.ir'
    access_key = 'ab5fc903-7426-4a49-ae3e-024b53c30d27'
    secret_key = 'f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2'
    bucket_name = 'qbc'
    s3_resource = boto3.resource(
        's3', endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key,
    )

    return s3_resource, bucket_name


def get_new_files(start_date, file_extensions):
    """
    Retrieve new files from an S3 bucket that match the specified extensions and were created on the execution date.
    """
    s3_resource, bucket_name = connected_to_s3()
    bucket = s3_resource.Bucket(bucket_name)

    new_files = []

    # Calculate the date range for the execution date (e.g., files created on that day)
    tz = pytz.timezone('UTC')  # Adjust this to your desired timezone
    if start_date.tzinfo is None:
        start_date = tz.localize(start_date)

    logger.info(f'#### {start_date} - {type(start_date)}')
    # Iterate through all objects in the bucket
    for obj in bucket.objects.all():
        if any(obj.key.endswith(ext) for ext in file_extensions):
            file_last_modified = obj.last_modified
            if start_date <= file_last_modified:
                new_files.append(obj.key)

    return new_files


def parse_datetime(value):
    # If the value is already a datetime object, return it as-is
    if isinstance(value, datetime):
        return value

    # If the value is a string, try to parse it
    elif isinstance(value, str):
        try:
            # Handle ISO formatted with 'T' separator (e.g., "2023-10-01T12:34:56.789Z")
            if "T" in value:
                # Try parsing with microseconds and timezone
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    # Fallback to strptime for older Python versions or non-ISO formats
                    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
            else:
                # Handle non-ISO format (e.g., "2023-10-01 12:34:56")
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # If parsing fails, return a default datetime (January 1, 1970, UTC)
            return datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)

    # If the value is not a datetime or string, return a default datetime
    return datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
