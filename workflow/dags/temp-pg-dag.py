from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from tasks.etl_tracking_csv import create_tracking_table
from tasks.etl_process_s3_csv import process_csv_files
from tasks.pg_to_clickhouse import transfer_data_in_batches

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_pg_TEST',
    default_args=default_args,
    description='ETL process for channel data from S3 to PostgreSQL',
    schedule_interval='30 15 * * *',
    catchup=False,
)

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