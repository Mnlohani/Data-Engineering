#Goal:
    # We want an ELT process to identify most highly traffic locations from a csv file in S3 which contains
    # traffic data of all cities.

# Solution:
    # For this, we will build an Airflow DAG that allows us to perform this analysis. 
    # Steps:
    # 1. Set up a connection RedShift to Airflow.
    # 2. Define a function "Load_data to RedShift".
    # 3. Create trips table in RedShift
        # Set up  PostgresOperator ( We are working with Redshift but 
        # using PostgresOperator since Postgres and RedShift are interOperable from
        # the perspective of the Airflow) and run SQL with sql.xxxxx
        # Give connection_id name : In airflow, create variables such as connection_id for db connections
        # for RedShift to Airflow
    # 4. Define copy_task that loads data into Reshift "trips" table which uses "Load_data to RedShift" task.
    # 5. Add another operator which creates a traffic analysis table from the "trips" table we created in RedShift.
    # 6. Define dependencies which task comes before which task.


#Prerequist:
    # Create an AWS Redshift Cluster
        # Cluster configuration:
            # choose free trial
        # Database configurations:
            # Input Admin user name. This can be same as the IAM user you created earlier.
            # Input Admin user password. Keep the username and password saved locally, as they will be needed in Airflow.
            # Click on Create cluster.
        # Modify publicly accessible setting: enable
        # Enable VPC Routing by going to the Properties tab and clicking on Edit button in the Network and Security settings section.
        # Choose the link next to VPC security group to open the Amazon Elastic Compute Cloud (Amazon EC2) console.
        # Go to Inbound Rules tab and click on Edit inbound rules.
            # Add an inbound rule, as
                # Type = Custom TCP
                # Port range = 0 - 5500
                # Source = Anywhere-iPv4
        # SUCCESS: Now our Redshift cluster should be accessible from Airflow.
        # Go back to the Redshift cluster and copy the endpoint

    # Add Airflow Connection to AWS Redshift
        # Go to Admin >> Connections >> Create
        # Conn Id: Enter redshift.
        # Conn Type: Enter Postgres.
        # Host: Enter the endpoint of your Redshift cluster, excluding the port and schema name at the end.
        # Schema: Enter dev. This is the Redshift database you want to connect to.
        # Login: Enter loginId
        # Password: Enter the password you created when launching your Redshift cluster.
        # Port: Enter 5439. Once you've entered these values, select Save.

# Awesome! We are now all configured to run Airflow with Redshift.

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hooks import AwsHook
from airflow.hooks.postgres_hooks import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements


def load_data_to_redshift(*args, **kwargs):
    aws_hook =  AwsHook("aws_credentials") # name of the connection variable
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))

dag = DAG(
    'Analyse_most_traffic_points',
    start_date=datetime.datetime.now())

# define tasks using different operators

create_table_in_redshift_task = PostgresOperator(
    task_id="create_table in RedShift",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL)

# Workflow:
# sql_statements.CREATE_TRIPS_TABLE_SQL
'''CREATE_TRIPS_TABLE_SQL = 
CREATE TABLE IF NOT EXISTS trips (
trip_id INTEGER NOT NULL, ...) 
'''


load_from_s3_to_redshift_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift)


# Workflow:
# python_callable=load_data_to_redshift
# redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))
'''
COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key)

COPY_ALL_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)
'''
'''
COPY_SQL = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
IGNOREHEADER 1
DELIMITER ','
"""
'''

analysis_location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

# define dependencies
create_table_in_redshift_task >> load_from_s3_to_redshift_task
load_from_s3_to_redshift_task >> analysis_location_traffic_task


