# Data Engineering Nanodegree Program

## Summary

* [Purpose](#Purpose)
* [Program Syllabus](#Program-Syllabus)
	* [Data Modeling](#Data_Modeling)
	* [Cloud Data Warehouses](#Cloud-Data-Warehouses)
	* [Data Lakes with Spark](#Data-Lakes-with-Spark)
	* [Data Pipelines with Airflow](#Data-Pipelines-with-Airflow)
	* [Capstone Project](#Capstone-Project)
* [Project structure](#Project-structure)
--------------------------------------------

### Purpose
Sparkify is a music streaming startup which provides free and premium plans. Sparkify is using .json files to store their song and user data. This project implements more efficient and sophisticated data warehouse that will be used in future analysis of the data to gain insights and implement necessary bussiness decisions in order to convert free plans to premium plans as much as possible.

### Program Syllabus

#### Data Modeling

Learn to create relational and NoSQL data models to fit the diverse needs of data consumers. Use ETL to build databases in PostgreSQL and Apache Cassandra.

_**Project:** Data Modeling with Postgres_
_**Project:** Data Modeling with Apache Cassandra_

#### Cloud Data Warehouses

Introduction to cloud computing, setting up a AWS workspace, learn to implement data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. Load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

_**Project:** Data Warehouse_

#### Data Lakes with Spark

Learn about the problems that Apache Spark is designed to solve, introduction to Big Data Ecosystem. Cleaning and aggregating the data in Spark, introduction troubleshooting techinques and potential ways to optimize performance of Spark aplications. Implement what is learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. Load data from S3, process the data into analytics tables using Spark, and load them back into S3. Deploy this Spark process on a cluster using AWS.

_**Project:** Data Lake_

#### Data Pipelines with Airflow

Introduction to data pipelines, Apache Airflow as data pipeline solution, how Airflow works, how to configure and scheduling data pipelines with Airflow, and debug a pipeline job. How to track data lineage and set up data pipeline schedules, partition the data to optimize pipelines, investigating Data Quality issues and write tests to ensure data quality. Learn to build the pipelines with maintainability and reusability in line, and pipeline monitoring.

_**Project:** Data Pipelines_

#### Capstone Project

_**Project:** Data Engineering Capstone Project_

### Project structure
```
data_engineering/
├── Exercises
│   ├── Exercises\ 1\ -\ Data\ Modeling
│   │   └── ...
│   ├── Exercises\ 2\ -\ Data\ Warehousing
│   │   └── ...
│   ├── Exercises\ 3\ -\ Infrastructure\ as\ Code
│   │   └── ...
│   └── Exercises\ 4\ -\ Data\ Lake\ with\ Spark
│       └── ...
├── Projects
│   ├── Project\ 1\ -\ Data\ Modeling\ with\ Postgre
│   │   └── ...
│   ├── Project\ 2\ -\ ETL\ Pipeline\ for\ Pre-Processing\ the\ Files
│   │   └── ...
│   ├── Project\ 3\ -\ Data\ Warehouse
│   │   └── ...
│   └── Project\ 4\ -\ Data\ Lake\ with\ Spark
│       └── ...
└── README.md
```

