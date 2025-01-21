## Overview

This `Dockerfile` sets up the environment needed to run Apache Airflow along with the necessary dependencies for interacting with MongoDB and ClickHouse.

## Dockerfile Explanation

- **Base Image**: The Dockerfile uses the official Apache Airflow image version 2.10.4 as the base, which includes a pre-configured environment for running Apache Airflow.
- **Root User**: It switches to the root user to install necessary packages and updates.
- **Install Sudo**: Updates package lists and installs the `sudo` package for granting temporary superuser privileges.
- **Install Build Tools**: Installs `build-essential`, which includes tools required for building and compiling software.
- **Upgrade Pip, Setuptools, and Wheel**: Upgrades Python utilities `pip`, `setuptools`, and `wheel` to their latest versions.
- **Install Python Packages**: Installs specific versions of necessary Python packages:
  - `apache-airflow-providers-mongo` (version 3.0.0)
  - `pymongo` (version 3.11.4)
  - `clickhouse-driver`
- **Airflow User**: Switches back to the airflow user for running processes with non-root privileges.

## Installation

### Using Docker and Docker Compose

1. Clone this repository.
2. Build and deploy the Docker image using the provided Dockerfile:

```sh
  docker-compose up --build
```

## Docker Compose Configuration

The `docker-compose.yml` file sets up the necessary services and their configurations for running Airflow with MongoDB and ClickHouse.

### Services

1. **Postgres**:
    - Uses the `postgres:13` image.
    - Sets environment variables for user and password.
    - Maps volumes for data persistence and initialization scripts.
    - Includes healthchecks and resource limits.
2. **Redis**:
    - Uses the `redis:7.2-bookworm` image.
    - Includes healthchecks and sets the exposed port to `6379`.
3. **Mongo**:
    - Uses the `mongo:4.4` image.
    - Sets environment variables for root user and password.
    - Maps volumes for data persistence.
    - Includes healthchecks and sets the exposed port to `27017`.
4. **ClickHouse**:
    - Uses the `clickhouse/clickhouse-server:latest` image.
    - Sets environment variables for user and password.
    - Maps volumes for data persistence.
    - Includes Ulimits settings for file descriptors.
5. **Airflow Services (Webserver, Scheduler, Worker, Triggerer, Init)**:
    - All services use a common setup defined in the `x-airflow-common` configuration.
    - Environment variables set for Airflow configuration.
    - Volumes are mapped for Airflow directories such as `dags`, `logs`, `config`, `plugins`, `tasks`, and `utils`.
    - Each service has specific commands (`webserver`, `scheduler`, `celery worker`, `triggerer`) and healthchecks.
    - The `airflow-init` service ensures proper initialization and user setup.
6. **Flower** (Optional):
    - Uses the Airflow common setup to run Flower for monitoring Celery workers.
    - Exposes port `5555`.

## Usage

1. Place the DAG file in your Airflow DAGs folder.
2. Start the Airflow services using Docker Compose:

    ```sh
    docker-compose up -d
    ```

3. Access the Airflow web interface at `http://localhost:9090`.
4. Trigger the DAG manually or wait for the scheduled run.

## Notes

- Ensure that your Airflow connections are properly set up for MongoDB (`MONGO_CONN_ID`) and ClickHouse.
- Adjust the included packages in the Dockerfile based on your environment and requirements.
