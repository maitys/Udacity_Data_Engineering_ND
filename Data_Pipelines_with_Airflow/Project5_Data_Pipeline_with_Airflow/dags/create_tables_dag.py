# This code creates an Airflow DAG named "create_table_dag" with a start date of the current time, and default arguments defined in a separate default_args variable.
# It uses the PostgresOperator to execute a SQL command that creates a table in a Redshift database. 
# The connection to the Redshift database is established using the postgres_conn_id "redshift".
# The SQL command to create the table is read from a file named "create_tables.sql".
# It creates a task named "create_table" that will be executed by the DAG.
# This DAG will only run this task once and the task is responsible for creating a table in the Redshift database using the sql command from the file.

import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from udac_example_dag import default_args


with DAG('create_table_dag', 
         start_date=datetime.datetime.now(),
         default_args=default_args) as dag:
    
    PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql='create_tables.sql'
    )