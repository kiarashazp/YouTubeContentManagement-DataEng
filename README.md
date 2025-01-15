# YouTubeContentManagement-DataEng

 پروژه در رابطه با مدیریت محتوای یوتیوب است. هدف پروژه راه‌اندازی و مدیریت یک پایپ‌لاین داده که شامل بارگذاری، پردازش، و تحلیل داده‌های استخراج‌شده از یوتیوب است. با PostgreSQL و MongoDB را تنظیم کنیم، داده‌ها را به روش های متفاوتی بارگذاری کنیم، از Airflow برای مدیریت جریان کار استفاده کنیم و در نهایت داده‌ها را به ClickHouse برای تحلیل نهایی منتقل کنیم.

# Airflow Data Pipeline

## Project Overview
This project sets up and manages a data pipeline using Apache Airflow. The pipeline extracts, transforms, and loads data from PostgreSQL and MongoDB into ClickHouse for analysis and reporting.

## Directory Structure
    YouTubeContentManagement-DataEng/
        config/
        mongodb/
            init_data_mongo.py
            README.md
            representational.py
        sql/
            postgres/
            clickhouse/
        docker/
            Dockerfile
            docker-compose.yaml
        workflow/
            tasks/
            utils/
            dags/
        requirements.txt
        README.md