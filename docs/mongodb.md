# MongoDB Initialization and Data Transformation

By following these steps, we can ensure that our MongoDB instance is properly initialized and that data is transformed and inserted as required.

## How It Works

1. **Data Initialization Script (`init_data_mongo.sh`):**
	- This Bash script starts MongoDB in the background and uses the `mongorestore` command to insert video data into MongoDB.

2. **Data Transformation Script (`representational.py`):**
	- First, unzips the file on the server using the command `tar -xzf ${file_name_with_tar.gz_extension}`.
	- Converts BSON to JSON and then transforms it into the desired schema with a Python script (this script is `representational.py`).
	- After transformation, imports the desired schema (JSON file) into a MongoDB database.
	- Finally, uses the `mongorestore` command to insert BSON and metadata files into the database.



