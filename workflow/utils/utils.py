import logging
from clickhouse_driver import Client
import boto3
from datetime import datetime
import pytz
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


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

def create_clickhouse_schema():
    client = Client(host='clickhouse', user='airflow', password='airflow')

    client.execute('CREATE DATABASE IF NOT EXISTS bronze')

    client.execute('''
    CREATE TABLE IF NOT EXISTS bronze.videos (
        id String,
        owner_username String,
        owner_id String,
        title String,
        tags Nullable(String),
        uid String,
        visit_count Int64,
        owner_name String,
        duration Int32,
        comments Nullable(Int32),
        like_count Nullable(Int64),
        is_deleted Bool,
        created_at DateTime64,
        expire_at DateTime64,
        update_count Int32
    ) ENGINE = MergeTree() PRIMARY KEY (id) ORDER BY id
    ''')

    logger.info("ClickHouse schema created successfully.")
    client.disconnect()

def load_query_from_file(file_path):

    with open(file_path, 'r') as file:
        query = file.read()
    return query

def prepare_batch_data(batch):
    """
    Prepare a batch of documents for insertion into ClickHouse.

    Args:
        batch (list): A list of documents (dictionaries) from MongoDB.

    Returns:
        list: A list of tuples in the format required for ClickHouse insertion.
    """
    return [
        (
            doc['id'],
            doc['owner_username'],
            doc['owner_id'],
            doc['title'],
            doc['tags'],
            doc['uid'],
            doc['visit_count'],
            doc['owner_name'],
            doc['duration'],
            doc['comments'],
            doc['like_count'],
            doc['is_deleted'],
            doc['created_at'],
            doc['expire_at'],
            doc['update_count']
        )
        for doc in batch
    ]

def process_dataframe(df: pd.DataFrame, file_name: str):
    required_columns = ['_id', 'created_at']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing required columns in {file_name}")

    df = df[required_columns].copy()
    df.columns = ['username', 'created_at']

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO channels (
                    username, total_video_visit, video_count,
                    start_date_timestamp, followers_count, country,
                    created_at, update_count
                ) VALUES (
                    %s, 0, 0, 1672531200, 0, '-', %s, 0
                )
                ON CONFLICT (username) DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    update_count = channels.update_count + 1
            """, (row['username'], row['created_at']))
        conn.commit()

def update_tracking_table(file_name: str):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO csv_files (file_name, inserted)
            VALUES (%s, TRUE)
            ON CONFLICT (file_name) DO UPDATE SET inserted = TRUE
        """, (file_name,))
        conn.commit()
