# PostgreSQL to ClickHouse Batch Load

## Overview

This project contains an Apache Airflow DAG for batch loading data from PostgreSQL to ClickHouse. 
The DAG is designed to transfer data in batches to ensure efficient and timely data migration.

## Requirements

- Docker and Docker Compose

## DAG and Task Details 

### DAG: postgres_to_clickhouse_batch_load 
- **Description**: Batch load data from PostgreSQL to ClickHouse 
- **Schedule Interval**: None (manual trigger) 
- **Start Date**: `days_ago(1)` 
- **Catchup**: False 


### Default Arguments 
- `owner`: `'airflow'` 
- `depends_on_past`: `False` 
- `email_on_failure`: `False` 
- `email_on_retry`: `False`


### Tasks

1. **Task: transfer_data_in_batches**
   - **Task ID**: `transfer_data_in_batches`
   - **Python Callable**: `transfer_data_in_batches`
   - **Provide Context**: `True`

2. **Task: load_clickhouse_batch**
   - **Task ID**: `load_clickhouse_batch`
   - **Python Callable**: `load_clickhouse_batch`
   - **Provide Context**: `True`

3. **Task: extract_postgres_batch**
   - **Task ID**: `extract_postgres_batch`
   - **Python Callable**: `extract_postgres_batch`
   - **Provide Context**: `True`


## Task Details

### transfer_data_in_batches
The `transfer_data_in_batches` task is responsible for managing the batch transfer process from PostgreSQL to ClickHouse.

- **Batch Size**: Configurable through the `BATCH_SIZE_POSTGRES` variable (default: 1000).
- **Extract Data**: Uses the `PostgresHook` to extract data from PostgreSQL in batches.
- **Load Data**: Executes ClickHouse schema initialization and loading batch data using `load_clickhouse_batch`.

### load_clickhouse_batch
The `load_clickhouse_batch` task is responsible for loading batch data into ClickHouse from a PostgreSQL source.

- **Uses** the `clickhouse_driver.Client` to interact with ClickHouse.
- **Loads** the SQL query from the specified file (`load_clickhouse_query_path`).
- **Database credentials**:
    - Host: `clickhouse`
    - Port: 9000
    - User: `airflow`
    - Password: `airflow`
    - Database: `bronze`
- **Processes** the batch data and executes the query on ClickHouse.

### extract_postgres_batch
The `extract_postgres_batch` task is responsible for extracting batch data from PostgreSQL.

- **Uses** the `PostgresHook` to connect and query PostgreSQL.
- **Extracts** data in batches based on the specified batch size and offset.


# MongoDB to ClickHouse Airflow DAG

## Overview

This repository contains an Apache Airflow DAG that transfers data from MongoDB to ClickHouse. The DAG consists of tasks for creating the schema in ClickHouse and performing data extraction and loading from MongoDB to ClickHouse.

## Requirements

- Docker and Docker Compose

## DAG Details

### Tasks

1. **Create ClickHouse Schema**: This task creates the necessary database and table schema in ClickHouse. It sets up a schema named `bronze` and a table `videos`.

2. **Extract Data**: This part of the `read_and_load` task queries MongoDB for data using batch processing. The batch size is determined by an Airflow Variable (`mongo_batch_size`), with a default value of 1000.

3. **Load Data**: After extracting the data, this task loads the data into ClickHouse. It inserts documents into the `bronze.videos` table, ensuring that each document's fields are correctly formatted and inserted.

### Default Arguments

- **Owner**: `airflow`
- **Start Date**: `2023-01-01`
- **Retries**: `1`
- **Schedule Interval**: `@once`.

### Variables

- `mongo_batch_size`: Specifies the batch size for reading data from MongoDB. Defaults to `1000`.


## Usage

1. Ensure that your Docker environment is set up and the necessary services are running using Docker Compose:
    ```sh
    docker-compose up
    ```
2. Wait for the Docker Compose services (Airflow webserver, scheduler, etc.) to be fully up and running.
3. Place the DAG file in your Airflow DAGs folder.
4. Access the Airflow web interface at `http://localhost:9090`.
5. Trigger the DAG manually or wait for the scheduled run.

## Notes

- Ensure that your Airflow connections are properly set up for MongoDB (`MONGO_CONN_ID`) and ClickHouse.
- Adjust the included packages or settings based on your environment and requirements.
