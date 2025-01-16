from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from utils.load_sql_query import load_query_from_file


def extract_postgres_batch(batch_size, offset, **context):

    query_file_path = Variable.get("extract_postgres_query_path")

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    query = load_query_from_file(query_file_path)

    result = pg_hook.get_records(query, parameters=(batch_size, offset))
    print(result)
    context['ti'].xcom_push(key='batch_data', value=result)