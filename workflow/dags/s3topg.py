from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os
import logging
import traceback

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_csv_from_s3',
    default_args=default_args,
    description='Fetch CSV files from Arvan Cloud S3 bucket',
    schedule_interval='30 15 * * *',
    catchup=False,
)

def fetch_directories_from_s3(bucket_name, prefix, local_base_dir, **context):
    logging.info("Starting function execution")
    logging.info(f"Bucket: {bucket_name}")
    logging.info(f"Prefix: {prefix}")
    logging.info(f"Local base dir: {local_base_dir}")
    
    # Test directory permissions
    try:
        logging.info(f"Testing if directory {local_base_dir} exists")
        if not os.path.exists(local_base_dir):
            logging.info(f"Creating directory {local_base_dir}")
            os.makedirs(local_base_dir)
        
        # Test write permissions
        test_file = os.path.join(local_base_dir, 'test.txt')
        logging.info(f"Testing write permissions with {test_file}")
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        logging.info("Directory permissions test passed")
    except Exception as e:
        logging.error(f"Directory permission error: {str(e)}")
        logging.error(traceback.format_exc())
        raise
    
    try:
        execution_date = context['execution_date']
        logging.info(f"Execution date: {execution_date}")
        
        # Initialize S3 client
        try:
            logging.info("Initializing S3 client")
            s3 = boto3.client(
                's3',
                aws_access_key_id='ab5fc903-7426-4a49-ae3e-024b53c30d27',
                aws_secret_access_key='f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2',
                endpoint_url='https://s3.ir-thr-at1.arvanstorage.ir'
            )
            logging.info("S3 client initialized successfully")
        except Exception as e:
            logging.error(f"S3 client initialization error: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
        # Test S3 connection
        try:
            logging.info("Testing S3 connection")
            s3.list_buckets()
            logging.info("S3 connection test successful")
        except Exception as e:
            logging.error(f"S3 connection test failed: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
        # List objects
        try:
            logging.info(f"Listing objects in bucket {bucket_name} with prefix {prefix}")
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            keys = [obj['Key'] for obj in response.get('Contents', [])]
            logging.info(f"Found {len(keys)} objects")
        except Exception as e:
            logging.error(f"Error listing S3 objects: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
        if not keys:
            logging.warning(f"No keys found in bucket '{bucket_name}' with prefix '{prefix}'")
            return
            
        # Process files
        directories = {}
        for key in keys:
            logging.info(f"Processing key: {key}")
            key_parts = key.split('/')
            if len(key_parts) >= 1:
                dir_part = key_parts[0] if len(key_parts) > 1 else ''
                try:
                    key_date_str = dir_part.split()[0]
                    key_date = datetime.strptime(key_date_str, '%Y-%m-%d')
                    
                    # Convert key_date to timezone-aware
                    key_date = key_date.replace(tzinfo=execution_date.tzinfo) 
                    
                    if key_date <= execution_date:
                        if dir_part not in directories:
                            directories[dir_part] = []
                        directories[dir_part].append(key)
                except ValueError as e:
                    logging.warning(f"Skipping key with invalid date format: {key}")
                    continue
        
        # Download files
        for dir_date_str, dir_keys in directories.items():
            local_date_dir = os.path.join(local_base_dir, dir_date_str.replace(' ', '_'))
            try:
                os.makedirs(local_date_dir, exist_ok=True)
            except Exception as e:
                logging.error(f"Error creating directory {local_date_dir}: {str(e)}")
                raise
            
            for key in dir_keys:
                if key.endswith('.csv'):
                    local_file_path = os.path.join(local_date_dir, os.path.basename(key))
                    if not os.path.exists(local_file_path):
                        try:
                            logging.info(f"Downloading {key} to {local_file_path}")
                            s3.download_file(
                                Bucket=bucket_name,
                                Key=key,
                                Filename=local_file_path
                            )
                            logging.info(f"Successfully downloaded {key}")
                        except Exception as e:
                            logging.error(f"Error downloading {key}: {str(e)}")
                            logging.error(traceback.format_exc())
                            raise
                else:
                    logging.info(f"Skipping non-CSV file: {key}")
                    
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        logging.error(traceback.format_exc())
        raise

fetch_directories_task = PythonOperator(
    task_id='fetch_directories_from_s3',
    python_callable=fetch_directories_from_s3,
    op_kwargs={
        'bucket_name': 'qbc',
        'prefix': '',
        'local_base_dir': '/data/s3',
    },
    dag=dag,
)