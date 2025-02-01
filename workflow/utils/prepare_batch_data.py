def prepare_batch_data(batch):
    """
    Prepare a batch of documents for insertion into ClickHouse.

    Args:
        batch (list): A list of documents (dictionaries) from MongoDB.

    Returns:
        list: A list of tuples in the format required for ClickHouse insertion.
    """
    return [
        (
            doc['id'],
            doc['owner_username'],
            doc['owner_id'],
            doc['title'],
            doc['tags'],
            doc['uid'],
            doc['visit_count'],
            doc['owner_name'],
            doc['duration'],
            doc['comments'],
            doc['like_count'],
            doc['is_deleted'],
            doc['created_at'],
            doc['expire_at'],
            doc['update_count']
        )
        for doc in batch
    ]
