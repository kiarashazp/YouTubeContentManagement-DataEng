def load_query_from_file(file_path):

    with open(file_path, 'r') as file:
        query = file.read()
    return query
