from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import logging
import traceback
import psycopg2
import pandas as pd
import io
from tasks.pg_to_clickhouse import transfer_data_in_batches

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_pg_etl',
    default_args=default_args,
    description='ETL process for channel data from S3 to PostgreSQL',
    schedule_interval='30 15 * * *',
    catchup=False,
)

DB_CONFIG = {
    'dbname': 'channels_data',
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres',
    'port': '5432'
}

S3_CONFIG = {
    'bucket_name': 'qbc',
    'prefix': '',
    'aws_access_key_id': 'ab5fc903-7426-4a49-ae3e-024b53c30d27',
    'aws_secret_access_key': 'f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2',
    'endpoint_url': 'https://s3.ir-thr-at1.arvanstorage.ir'
}

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

def create_tracking_table(**context):
    logging.info("Creating/verifying tracking table")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS csv_files (
                        file_name TEXT PRIMARY KEY,
                        inserted BOOLEAN DEFAULT FALSE
                    )
                """)
            conn.commit()
        logging.info("Tracking table created/verified")
    except Exception as e:
        logging.error(f"Error creating tracking table: {str(e)}")
        raise

def process_csv_files(**context):
    logging.info("Starting S3 CSV processing")
    execution_date = context['execution_date']
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_CONFIG['aws_access_key_id'],
        aws_secret_access_key=S3_CONFIG['aws_secret_access_key'],
        endpoint_url=S3_CONFIG['endpoint_url']
    )

    # Get processed files
    with get_db_connection() as conn:
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
                response = s3.get_object(Bucket=S3_CONFIG['bucket_name'], Key=key)
                df = pd.read_csv(io.BytesIO(response['Body'].read()))

                process_dataframe(df, key)
                update_tracking_table(key)
                logging.info(f"Successfully processed {key}")

            except Exception as e:
                logging.error(f"Failed to process {key}: {str(e)}")
                logging.error(traceback.format_exc())
                raise

def process_dataframe(df: pd.DataFrame, file_name: str):
    required_columns = ['_id', 'created_at']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing required columns in {file_name}")

    df = df[required_columns].copy()
    df.columns = ['username', 'created_at']

    with get_db_connection() as conn:
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
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO csv_files (file_name, inserted)
                VALUES (%s, TRUE)
                ON CONFLICT (file_name) DO UPDATE SET inserted = TRUE
            """, (file_name,))
            conn.commit()

create_csv_tracking_table = PythonOperator(
    task_id='create_tracking_table',
    python_callable=create_tracking_table,
    dag=dag,
)

process_s3_files = PythonOperator(
    task_id='process_s3_files',
    python_callable=process_csv_files,
    provide_context=True,
    dag=dag,
)

transfer_to_clickhouse = PythonOperator(
    task_id='transfer_data_to_clickhouse',
    python_callable=transfer_data_in_batches,
    provide_context=True,
    dag=dag,
)

create_csv_tracking_table >> process_s3_files >> transfer_to_clickhouse