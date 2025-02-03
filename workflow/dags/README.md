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

### Schedule Interval

The DAG is scheduled to run once (`@once`).

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

