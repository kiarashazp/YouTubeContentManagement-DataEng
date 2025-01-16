from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from tasks import create_clickhouse_schema
from tasks import extract_postgres_data
from tasks import extract_mongo_data
from tasks import load_to_clickhouse_bronze
from tasks import process_to_silver_layer
from tasks import generate_reports

# Default arguments for the DAG
default_args = {
    'owner': 'workflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG with a one-time schedule
dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='A one-time data pipeline DAG for extracting, processing, and analyzing data',
    schedule_interval='@once',
)

create_schema_task = PythonOperator(
    task_id='create_clickhouse_schema',
    python_callable=create_clickhouse_schema,
    dag=dag,
)

# Define the PostgreSQL extraction task
extract_postgres_task = PythonOperator(
    task_id='extract_postgres_data',
    python_callable=extract_postgres_data,
    dag=dag,
)

# Define the MongoDB extraction task
extract_mongo_task = PythonOperator(
    task_id='extract_mongo_data',
    python_callable=extract_mongo_data,
    dag=dag,
)


# Define the Bronze layer loading task
load_bronze_task = PythonOperator(
    task_id='load_to_clickhouse_bronze',
    python_callable=load_to_clickhouse_bronze,
    provide_context=True,
    dag=dag,
)

# Define the Silver layer processing task
process_to_silver_task = PythonOperator(
    task_id='process_to_silver_layer',
    python_callable=process_to_silver_layer,
    dag=dag,
)

# Define the Gold layer reporting task
generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

# Set task dependencies
create_schema_task >> [extract_postgres_task, extract_mongo_task] >> load_bronze_task >> process_to_silver_task >> generate_reports_task
