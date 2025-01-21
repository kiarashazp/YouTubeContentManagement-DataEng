# Explain Part Mongo

This folder contains MongoDB initialization script for our. insert the `videos data` by run init_data_mongo.sh.
Also, another file as representational.py, that convert data to our desired schema.

1. ## docker-compose.yaml
   - Defines a `mongodb_container` service that mount
     - A named volume `mongo_data` for database persistence.

2. ## init_data_mongo.sh
    - in bash script start mongo in background
    - with command mongorestore insert data video in mongodb

3. ## representational.py
    - First, we unzipped the file on the server with the `tar -xzf ${file_name_with_tar.gz_extension}` command
    - Then we converted the bson to Json and then converted it to the desired schema with Python script (this script is representational.py).
    - After that, we imported the desired schema, which was a Json file, into a Mongo database, and from that, we took the bson and metadata files to enter into our database on the server with the `mongorestore` command
