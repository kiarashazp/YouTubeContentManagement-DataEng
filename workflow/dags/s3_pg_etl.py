from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os
import logging
import traceback
import psycopg2
import pandas as pd
from typing import List

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
    schedule_interval='30 15 * * *', # to run at 19:00 tehran time
    catchup=False,
)

DB_CONFIG = {
    'dbname': 'channels_data',
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres',
    'port': '5432'
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

def create_tracking_table(**context):
    logging.info("Creating tracking table if not exists")
    
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
        logging.info("Tracking table created/verified successfully")
    except Exception as e:
        logging.error(f"Error creating tracking table: {str(e)}")
        raise

def download_new_files(bucket_name: str, prefix: str, local_base_dir: str, **context):
    """Download new CSV files and track them in the database"""
    logging.info("Starting download of new files")
    
    s3 = boto3.client(
        's3',
        aws_access_key_id='ab5fc903-7426-4a49-ae3e-024b53c30d27',
        aws_secret_access_key='f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2',
        endpoint_url='https://s3.ir-thr-at1.arvanstorage.ir'
    )
    
    # Get list of already processed files
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_name FROM csv_files")
            existing_files = {row[0] for row in cur.fetchall()}
    
    # List and filter S3 objects
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    execution_date = context['execution_date']
    
    for obj in response.get('Contents', []):
        key = obj['Key']
        if not key.endswith('.csv'):
            continue
            
        # Parse date from key
        key_parts = key.split('/')
        if len(key_parts) >= 1:
            dir_part = key_parts[0] if len(key_parts) > 1 else ''
            try:
                key_date_str = dir_part.split()[0]
                key_date = datetime.strptime(key_date_str, '%Y-%m-%d')
                key_date = key_date.replace(tzinfo=execution_date.tzinfo)
                
                # Download file if its date precedes the task's running date
                if key_date <= execution_date and key not in existing_files:
                    local_date_dir = os.path.join(local_base_dir, dir_part.replace(' ', '_'))
                    os.makedirs(local_date_dir, exist_ok=True)
                    local_file_path = os.path.join(local_date_dir, os.path.basename(key))
                    
                    logging.info(f"Downloading {key} to {local_file_path}")
                    s3.download_file(bucket_name, key, local_file_path)
                    
                    # Track file in database
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO csv_files (file_name) VALUES (%s)",
                                (key,)
                            )
                        conn.commit()
            except ValueError:
                logging.warning(f"Skipping key with invalid date format: {key}")
                continue

def process_csv_files(local_base_dir: str, **context):
    logging.info("Starting to process CSV files")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_name FROM csv_files WHERE NOT inserted")
                unprocessed_files = cur.fetchall()
                
        logging.info(f"Found {len(unprocessed_files)} unprocessed files")
        
        for (file_name,) in unprocessed_files:
            local_path = os.path.join(local_base_dir, 
                                    file_name.split('/')[0].replace(' ', '_'),
                                    os.path.basename(file_name))
            
            logging.info(f"Processing file: {local_path}")
            
            if not os.path.exists(local_path):
                logging.warning(f"File not found: {local_path}")
                continue
                
            process_single_file(local_path, file_name)
            
    except Exception as e:
        logging.error(f"Error in process_csv_files: {str(e)}")
        logging.error(traceback.format_exc())
        raise

def process_single_file(file_path: str, file_name: str):
    logging.info(f"Processing file: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Available columns: {df.columns.tolist()}")
        
        df = df[['_id', 'created_at']].copy()
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    # First check if username exists
                    cur.execute("""
                        SELECT username FROM channels WHERE username = %s
                    """, (row['_id'],))
                    
                    if cur.fetchone():
                        # Update existing record
                        cur.execute("""
                            UPDATE channels 
                            SET created_at = %s
                            WHERE username = %s
                        """, (row['created_at'], row['_id']))
                    else:
                        # Insert new record
                        cur.execute("""
                            INSERT INTO channels (
                                username, total_video_visit, video_count,
                                start_date_timestamp, followers_count, country,
                                created_at, update_count
                            ) VALUES (
                                %s, 0, 0, 1672531200, 0, '-', %s, 0
                            )
                        """, (row['_id'], row['created_at']))
                
                # Mark file as processed
                cur.execute("""
                    UPDATE csv_files 
                    SET inserted = TRUE 
                    WHERE file_name = %s
                """, (file_name,))
                
                conn.commit()
        
        # Remove processed file
        os.remove(file_path)
        logging.info(f"Completed processing file: {file_path}")
        
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {str(e)}")
        logging.error(traceback.format_exc())
        raise

create_csv_tracking_table = PythonOperator(
    task_id='create_tracking_table',
    python_callable=create_tracking_table,
    dag=dag,
)

download_csv_files = PythonOperator(
    task_id='download_new_files',
    python_callable=download_new_files,
    op_kwargs={
        'bucket_name': 'qbc',
        'prefix': '',
        'local_base_dir': '/data/s3',
    },
    dag=dag,
)

process_and_insert_csv_files = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    op_kwargs={
        'local_base_dir': '/data/s3',
    },
    dag=dag,
)

create_csv_tracking_table >> download_csv_files >> process_and_insert_csv_files