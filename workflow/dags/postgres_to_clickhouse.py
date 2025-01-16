from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from tasks.transfer_batches import transfer_data_in_batches

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


dag= DAG(
    'postgres_to_clickhouse_batch_load',
    default_args=default_args,
    description='Batch load data from PostgreSQL to ClickHouse',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)



transfer_task = PythonOperator(
    task_id='transfer_data_in_batches',
    python_callable=transfer_data_in_batches,
    provide_context=True,
    dag=dag
)

transfer_task
