from tasks.extract_postgres_data import extract_postgres_batch
from tasks.load_clickhouse import load_clickhouse_batch
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from utils.load_sql_query import load_query_from_file
from clickhouse_driver import Client


def transfer_data_in_batches(**context):

    batch_size = int(Variable.get("BATCH_SIZE_POSTGRES", default_var=1000))

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    total_rows_query = "SELECT COUNT(*) FROM channels;"
    total_rows = pg_hook.get_first(total_rows_query)[0]

    offset = 0
    
    # query_file_path = Variable.get("init_clickhouse_query_path")
    query_file_path = "utils/clickhouse_schema_channels.sql"
    query = load_query_from_file(query_file_path)

    # clickhouse_host = Variable.get("CLICKHOUSE_HOST")
    # clickhouse_user = Variable.get("CLICKHOUSE_USER")
    # clickhouse_pass = Variable.get("CLICKHOUSE_PASS")
    # clickhouse_db = Variable.get("CLICKHOUSE_DB")

    client = Client(
        host='clickhouse',
        port=9000,
        user='airflow',
        password='airflow',
        database='channels'
    )
    
    client.execute(query)
    
    while offset < total_rows:
        extract_postgres_batch(batch_size, offset, **context)

        load_clickhouse_batch(**context)

        offset += batch_size