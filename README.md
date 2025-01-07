# YouTubeContentManagement-DataEng

 پروژه در رابطه با مدیریت محتوای یوتیوب است. هدف پروژه راه‌اندازی و مدیریت یک پایپ‌لاین داده که شامل بارگذاری، پردازش، و تحلیل داده‌های استخراج‌شده از یوتیوب است. با PostgreSQL و MongoDB را تنظیم کنیم، داده‌ها را به روش های متفاوتی بارگذاری کنیم، از Airflow برای مدیریت جریان کار استفاده کنیم و در نهایت داده‌ها را به ClickHouse برای تحلیل نهایی منتقل کنیم. در ادامه به مرور readme پر خواهد شد


# Airflow Data Pipeline

## Project Overview
This project sets up and manages a data pipeline using Apache Airflow. The pipeline extracts, transforms, and loads data from PostgreSQL and MongoDB into ClickHouse for analysis and reporting.

## Directory Structure
project-root/
│
├── dags/
│   ├── __init__.py
│   ├── data_extraction_to_bronze.py
│   ├── data_processing_to_silver.py
│   ├── data_analysis_reports.py
│   └── ... (other DAGs)
│
├── config/
│   ├── __init__.py
│   ├── config.py
│   └── secrets.env
│
├── utils/
│   ├── __init__.py
│   ├── db_utils.py
│   └── ... (other utility modules)
│
├── tasks/
│   ├── __init__.py
│   ├── extract_tasks.py
│   ├── transform_tasks.py
│   ├── load_tasks.py
│   └── ... (other task modules)
│
├── sql/
│   ├── init_schemas.sql
│   └── ... (other SQL scripts)
│
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── ... (other Docker-related files)
│
├── logs/
│   └── ... (Airflow logs)
│
└── README.md

## Setup and Installation 
### Prerequisites 
- Docker 
- Docker Compose 
- Python 3.6+ 
- Apache Airflow 2.x 

### Installation Steps 
1. **Clone the repository:
** ```

