# Hooks: connections can be accessed via hooks. 
# hooks provide a resuable interface to the external systemd and db's.
# It gives security to the system and stores the connections strings and secret_key 
# in our code

# In this python file, we demonstrate that how to connect airflow with 
# AWS via python and we list the content of S3 bucket. 

import datetime
import logging

from airflow import DAG
from airflow.models import Variables
from airflow.operators.python_operators import PythonOperator
from airflow.hooks.s3_hooks import S3Hook

## Pre-requist Steps:
    
    # create variables .
        # 1. Open your browser to localhost:8080 and open Admin->Variables
        # 2. Click "Create"
        # 3. Set "Key" equal to "s3_bucket" and set "Val" equal to "udacity-dend"
        # 4. Set "Key" equal to "s3_prefix" and set "Val" equal to "data-pipelines"
        # 5. Click save

    # Create an IAM User in AWS
        # In the Set permissions section, select Attach existing policies directly. 
        # Search and select the following policies:
            # AdministratorAccess
            # AmazonRedshiftFullAccess
            # AmazonS3FullAccess
        # download the download.csv
     
    # Add Airflow Connections
        # Go to Admin >> Connections >> Create
        # Conn Id: Enter aws_credentials.
        # Conn Type: Enter Amazon Web Services.
        # Login: Enter your Access key ID from the IAM User credentials we downloaded earlier.
        # Password: Enter your Secret access key from the IAM User credentials we downloaded earlier.

def list_keys():
    hook = S3Hook(aws_conn_id="aws_credentials")
    bucket = Variables.get('s3_bucket')
    prefix = Variables.get('s3_prefix')
    logging.info(f"Listing keys from {bucket}/{prefix}")
    keys=hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"-s3://{bucket}/{key}")


dag = DAG(
        'hooks_aws',
        description='airflow to RedShift',
        start_date=datetime.datetime.now())
        

list_tasks = PythonOperator(
    task_id="list_keys",
    Python_callable=list_keys,
    dag=dag)