# YouTubeContentManagement-DataEng

 پروژه در رابطه با مدیریت محتوای یوتیوب است. هدف پروژه راه‌اندازی و مدیریت یک پایپ‌لاین داده که شامل بارگذاری، پردازش، و تحلیل داده‌های استخراج‌شده از یوتیوب است. با PostgreSQL و MongoDB را تنظیم کنیم، داده‌ها را به روش های متفاوتی بارگذاری کنیم، از Airflow برای مدیریت جریان کار استفاده کنیم و در نهایت داده‌ها را به ClickHouse برای تحلیل نهایی منتقل کنیم. 

# Airflow Data Pipeline

## Project Overview
This project sets up and manages a data pipeline using Apache Airflow. The pipeline extracts, transforms, and loads data from PostgreSQL and MongoDB into ClickHouse for analysis and reporting.

## Directory Structure
project-root/
│
├── config/
│
│── mongodb/
│
├── sql/
├── ├── postgres/
│   ├── clickhouse/
│
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│
│── workflow/
│    ├──  tasks/
│    ├── utils/
│    ├── dags/
│
│── requirements.txt
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