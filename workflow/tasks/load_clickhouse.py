from clickhouse_driver import Client
from utils.load_sql_query import load_query_from_file
from airflow.models import Variable

def load_clickhouse_batch(**context):

    # query_file_path = Variable.get("load_clickhouse_query_path")
    query_file_path = "utils/load_clickhouse.sql"
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
        database='bronze'
    )

    batch_data = context['ti'].xcom_pull(key='batch_data')

    batch_data = [
        (
            row[0],  # id
            row[1],  # username
            row[2],  # total_video_visit
            row[3],  # video_count
            row[4],  # start_date_timestamp
            row[5],  # followers_count
            row[6],  # country
            row[7],  # created_at
            row[8]   # update_count
        )
        for row in batch_data
    ]

    client.execute(query, batch_data)