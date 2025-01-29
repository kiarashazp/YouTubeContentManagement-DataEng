from tasks.extract_mongo_data import extract_mongo_data


def transform_mongo_data(mongo_data: list[dict], **kwargs) -> list[dict]:
    """
    Transforms MongoDB data into a structured format for insertion into ClickHouse.

    Args:
        **kwargs: Airflow context containing 'logical_date', 'db_name', and 'collection_name'.

    Returns:
        list: A list of dictionaries containing transformed data.
    """
    try:
        # Extract data from MongoDB
        # ti = kwargs['ti']
        # mongo_data = ti.xcom_pull(task_ids='extract_mongo_data')
        logger.info(f"Transforming {len(mongo_data)} documents")

        # Initialize a list to store transformed data
        transformed_data = []

        # Transform each document
        for doc in mongo_data:
            videos_values = {
                'video_id': str(doc.get('id', '')),
                'owner_username': doc.get('owner_username', ''),
                'owner_id': doc.get('owner_id', ''),
                'title': doc.get('title', ''),
                'tags': doc.get('tags', ''),
                'uid': doc.get('uid', ''),
                'visit_count': doc.get('visit_count', 0),
                'owner_name': doc.get('owner_name', ''),
                'duration': doc.get('duration', 0),
                'posted_date': doc.get('posted_date', '1970-01-01'),
                'sdate_rss': doc.get('sdate_rss', '1970-01-01'),
                'comments': doc.get('comments', ''),
                'like_count': doc.get('like_count', 0),
                'is_deleted': doc.get('is_deleted', False),
                'created_at': doc.get('created_at', 0),
                'expire_at': doc.get('expire_at', 0),
                'update_count': doc.get('update_count', 0)
            }
            transformed_data.append(videos_values)

        logger.info(f"Successfully transformed {len(transformed_data)} documents")
        return transformed_data

    except Exception as e:
        logger.error(f"Error transforming MongoDB data: {e}")
        raise
