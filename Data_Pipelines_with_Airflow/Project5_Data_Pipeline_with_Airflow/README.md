# **Project 5 - Data Pipeline with Airflow**

## **Objective:**
Introduce additional automation for Sparkify using Airflow: A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. 

Note: The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## **How to achieve the Objective?**
In order to achieve this, we use our skills of working with Apache Airflow and build out a robust data pipeline.
We are required to create custom operators to complete tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## **Note about Workspace<br>**
After you have updated the DAG, you will need to run /opt/airflow/start.sh command to start the Airflow web server. Once the Airflow web server is ready, you can access the Airflow UI by clicking on the blue Access Airflow button.

## **Input Files:**
Song Data Path: s3://udacity-dend/song_data<br>
Log Data Path: s3://udacity-dend/log_data

## **Instructions on running the data pipeline**
- Setup a redshift cluster on AWS
- Start Airflow web server by running /opt/airflow/start.sh
- Setup AWS credentials in Airflow
- Setup redshift connection in Airflow
- Run the DAG to create the required empty tables in redshift
- Run the DAG to stage, load, and check the data in redshift 

## **Repo Structure**

        |____dags
        |     |____ create_tables_dag.py  # DAG for creating tables on Redshift
        |     |____ create_tables.sql     # SQL CREATE queries
        |     |____ udac_example_dag.py   # Main DAG for ETL data pipeline
        |
        |____plugins
        |     |____ operators
        |         |____ stage_redshift.py # COPY data from S3 to Redshift
        |         |____ load_fact.py      # Execute INSERT query into fact table
        |         |____ load_dimension.py # Execute INSERT queries into dimension tables
        |         |____ data_quality.py   # Data quality check after pipeline execution
        |     |____ helpers
        |         |____ sql_queries.py    # SQL queries for building dimensional tables


The dags directory contains the DAG definition files. These files define the overall structure of the pipeline, including the order of execution for the tasks and any dependencies between tasks. The create_tables_dag.py file defines a DAG for creating tables on Redshift, using the SQL statements in the create_tables.sql file. The udac_example_dag.py file defines the main DAG for the data pipeline, which will include tasks to execute the various stages of the pipeline.

The plugins directory contains custom Airflow plugins. In this case it contains operators and helpers directory. The operators directory contains custom operator classes that perform specific actions within the pipeline, such as the stage_redshift.py operator, which copies data from S3 to Redshift, or the load_fact.py operator, which inserts data into a fact table. Similarly, helpers directory contains helper functions and modules, such as sql_queries.py, which contains the SQL statements used to build the dimensional tables.