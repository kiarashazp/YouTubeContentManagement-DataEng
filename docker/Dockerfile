FROM apache/airflow:2.10.4

USER root

# Install sudo package
RUN apt-get update && apt-get install -y sudo

# Use sudo to run the build tools installation
RUN sudo apt-get update && sudo apt-get install -y build-essential

# Upgrade pip, setuptools, and wheel
RUN sudo pip install --upgrade pip setuptools wheel

# Install the required packages
RUN sudo pip install apache-airflow-providers-mongo==3.0.0 pymongo==3.11.4 clickhouse-driver

USER airflow
