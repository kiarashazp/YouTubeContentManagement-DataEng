import boto3
import logging
import traceback
import pandas as pd
import io
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.process_csv import process_dataframe, update_tracking_table

def process_csv_files(**context):
    logging.info("Starting S3 CSV processing")
    execution_date = context['execution_date']
    
    # Connect to S3
    S3_CONFIG = {
    'bucket_name': 'qbc',
    'prefix': '',
    'aws_access_key_id': 'ab5fc903-7426-4a49-ae3e-024b53c30d27',
    'aws_secret_access_key': 'f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2',
    'endpoint_url': 'https://s3.ir-thr-at1.arvanstorage.ir'
    }

    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_CONFIG['aws_access_key_id'],
        aws_secret_access_key=S3_CONFIG['aws_secret_access_key'],
        endpoint_url=S3_CONFIG['endpoint_url']
    )

    # Get processed files
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT file_name FROM csv_files WHERE inserted = TRUE")
        processed_files = {row[0] for row in cur.fetchall()}

    # List S3 objects
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=S3_CONFIG['bucket_name'], Prefix=S3_CONFIG['prefix'])

    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.csv'):
                continue

            if key in processed_files:
                logging.info(f"Skipping already processed file: {key}")
                continue

            # Parse date from key
            try:
                dir_part = key.split('/')[0]
                file_date = datetime.strptime(dir_part.split()[0], '%Y-%m-%d').replace(
                    tzinfo=execution_date.tzinfo
                )
                if file_date > execution_date:
                    logging.info(f"Skipping future-dated file: {key} ({file_date})")
                    continue
            except (IndexError, ValueError) as e:
                logging.warning(f"Skipping invalid key format: {key} - {str(e)}")
                continue

            # Process the file
            try:
                logging.info(f"Processing {key}")
                response = s3_resource.get_object(Bucket=bucket_name, Key=key)
                df = pd.read_csv(io.BytesIO(response['Body'].read()))

                process_dataframe(df, key)
                update_tracking_table(key)
                logging.info(f"Successfully processed {key}")

            except Exception as e:
                logging.error(f"Failed to process {key}: {str(e)}")
                logging.error(traceback.format_exc())
                raise
