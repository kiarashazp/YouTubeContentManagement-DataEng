import boto3
from datetime import datetime, timedelta
import pytz


def connected_to_s3():
    endpoint_url = 'https://s3.ir-thr-at1.arvanstorage.ir'
    access_key = 'ab5fc903-7426-4a49-ae3e-024b53c30d27'
    secret_key = 'f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2'
    bucket_name = 'qbc'
    s3_resource = boto3.resource(
        's3', endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key,
    )

    return s3_resource, bucket_name


def get_new_files(execution_date, file_extensions):
    """
    Retrieve new files from an S3 bucket that match the specified extensions and were created on the execution date.
    """
    s3_resource, bucket_name = connected_to_s3()
    bucket = s3_resource.Bucket(bucket_name)

    new_files = []

    # Calculate the date range for the execution date (e.g., files created on that day)
    tz = pytz.timezone('UTC')  # Adjust this to your desired timezone
    if execution_date.tzinfo is None:
        start_date = tz.localize(execution_date)
    else:
        start_date = execution_date

    end_date = start_date + timedelta(days=1)

    # Iterate through all objects in the bucket
    for obj in bucket.objects.all():
        if any(obj.key.endswith(ext) for ext in file_extensions):
            file_last_modified = obj.last_modified.replace(tzinfo=None)
            if start_date <= file_last_modified < end_date:
                new_files.append(obj.key)

    return new_files
