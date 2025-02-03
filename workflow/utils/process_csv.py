import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def process_dataframe(df: pd.DataFrame, file_name: str):
    required_columns = ['_id', 'created_at']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing required columns in {file_name}")

    df = df[required_columns].copy()
    df.columns = ['username', 'created_at']

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO channels (
                    username, total_video_visit, video_count,
                    start_date_timestamp, followers_count, country,
                    created_at, update_count
                ) VALUES (
                    %s, 0, 0, 1672531200, 0, '-', %s, 0
                )
                ON CONFLICT (username) DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    update_count = channels.update_count + 1
            """, (row['username'], row['created_at']))
        conn.commit()

def update_tracking_table(file_name: str):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO csv_files (file_name, inserted)
            VALUES (%s, TRUE)
            ON CONFLICT (file_name) DO UPDATE SET inserted = TRUE
        """, (file_name,))
        conn.commit()