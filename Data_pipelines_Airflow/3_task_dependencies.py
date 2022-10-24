import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("Hello world")


def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 -2 = {6-2}")
    

def division():
    logging.info(f"10 / 2 = {int(10/2)}")

dag = DAG(
    'excercise_3',
    schedule_interval="@hourly",
    start_date= datetime.datetime.now() - datetime.timedelta(days=1)
    )

# Define tasks to run function (python_callables) above
hello_world_task = PythonOperator(
    task_id="hello_world", # name of task for airflow GUI
    python_callable=hello_world, # name of the function to be exceuted
    dag= dag #name of the DAG 
    )
    
addition_task = PythonOperator(
     task_id="addition",
    python_callable=addition,
    dag=dag)

subtraction_task = PythonOperator(
    task_id="substraction",
    python_callable=subtraction,
    dag=dag)

division_task = PythonOperator(
    task_id="division",
    python_callable=division,
    dag=dag)

# Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

hello_world_task >> addition_task
hello_world_task >> subtraction_task
addition_task >> division_task
subtraction_task >> division_task
