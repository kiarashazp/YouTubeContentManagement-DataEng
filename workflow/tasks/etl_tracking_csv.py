import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_tracking_table(**context):
    logging.info("Creating/verifying tracking table")
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS csv_files (
                    file_name TEXT PRIMARY KEY,
                    inserted BOOLEAN DEFAULT FALSE
                )
            """)
            conn.commit()
        logging.info("Tracking table created/verified")
    except Exception as e:
        logging.error(f"Error creating tracking table: {str(e)}")
        raise