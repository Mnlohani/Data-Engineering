# define a function that uses python logger to log a function.
# start airflow webserver
# Turn the DAG "ON" and Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# define function for python Operator
def greet():
    #function for python Operator and log sth
    logging.info('Hello World')

# Define DAG
dag = DAG(
    'excercise_1',
    start_date=datetime.datetime.now()
)

# define task to instatiate Python Operator for DAG
greet_task = PythonOperator(
    task_id = 'greet_task', # name of the task for airflow
    python_callable=greet,
    dag=dag
)




