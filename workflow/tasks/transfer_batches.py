from tasks.extract_postgres_data import extract_postgres_batch
from tasks.load_clickhouse import load_clickhouse_batch
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


def transfer_data_in_batches(**context):

    batch_size = int(Variable.get("BATCH_SIZE_POSTGRES", default_var=1000))

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    total_rows_query = "SELECT COUNT(*) FROM channels;"
    total_rows = pg_hook.get_first(total_rows_query)[0]

    offset = 0
    while offset < total_rows:
        extract_postgres_batch(batch_size, offset, **context)

        load_clickhouse_batch(**context)

        offset += batch_size