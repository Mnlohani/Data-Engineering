# define a function that uses python logger to log a function.
# start airflow webserver
# Turn the DAG "ON" and Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# define function for python Operator
def hello_world():
    #function for python Operator and log sth
    logging.info('Hello World')

# Define DAG
# schedule it daily
dag = DAG(
    'excercise_2',
    start_date=datetime.datetime.now() - datetime.timedelta(days=2),
    schedule_interval="@daily")

# define task to instatiate Python Operator for DAG
greet_task = PythonOperator(
    task_id = 'greet_task', # name of the task for airflow
    python_callable=hello_world,
    dag=dag)




