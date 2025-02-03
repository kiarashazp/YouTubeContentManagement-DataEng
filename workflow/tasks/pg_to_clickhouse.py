from tasks.extract_postgres_data import extract_postgres_batch
from tasks.load_clickhouse import load_clickhouse_batch
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from utils.load_sql_query import load_query_from_file
from clickhouse_driver import Client
from datetime import datetime


def transfer_data_in_batches(**context):

    batch_size = int(Variable.get("BATCH_SIZE_POSTGRES", default_var=10000))
    
    query_file_path = "utils/clickhouse_schema_channels.sql"
    query = load_query_from_file(query_file_path)

    client = Client(
        host='clickhouse',
        port=9000,
        user='airflow',
        password='airflow',
        database='bronze'
    )
    client.execute(query)
    
    while True:
        
        run=extract_postgres_batch(batch_size, **context)
        if not run :
            break
        load_clickhouse_batch(**context)
