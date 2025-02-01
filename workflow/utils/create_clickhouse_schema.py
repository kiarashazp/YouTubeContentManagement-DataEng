from clickhouse_driver import Client
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_clickhouse_schema():
    client = Client(host='clickhouse', user='airflow', password='airflow')

    client.execute('CREATE DATABASE IF NOT EXISTS bronze')

    client.execute('''
    CREATE TABLE IF NOT EXISTS bronze.videos (
        id String,
        owner_username String,
        owner_id String,
        title String,
        tags Nullable(String),
        uid String,
        visit_count Int64,
        owner_name String,
        duration Int32,
        comments Nullable(Int32),
        like_count Nullable(Int64),
        is_deleted Bool,
        created_at DateTime64,
        expire_at DateTime64,
        update_count Int32
    ) ENGINE = MergeTree() PRIMARY KEY (id) ORDER BY id
    ''')

    logger.info("ClickHouse schema created successfully.")
    client.disconnect()