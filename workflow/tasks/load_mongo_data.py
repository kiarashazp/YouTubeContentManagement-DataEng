from utils.load_sql_query import load_query_from_file

def load_mongo_data(transformed_data list[dict], **kwargs) -> list[dict]:
    try:
        # Load the INSERT query from a file
        query_file_path = "utils/insert_videos_query.sql"
        insert_query = load_query_from_file(query_file_path)

        if not insert_query:
            logger.error("No SQL query loaded. Aborting.")
            return

        # Get transformed data
        # ti = kwargs['ti']
    	# transformed_data = ti.xcom_pull(task_ids='transform_mongo_data')

        if not transformed_data:
            logger.info("No data to load into ClickHouse.")
            return

        # Connect to ClickHouse and insert data
        client = Client(
            host='clickhouse',
            user='airflow',
            password='airflow'
        )
        
        client.execute(insert_query, transformed_data)
        logger.info(f"Successfully inserted {len(transformed_data)} rows into bronze.videos.")

    except Exception as e:
        logger.error(f"Failed to insert data into ClickHouse: {e}")
        raise
