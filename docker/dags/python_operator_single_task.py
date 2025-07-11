from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

# define py function to be executed


def greet():
    print("hello world")


with DAG(
    default_args=default_args,
    dag_id='python_operator_single_task',
    description='DAG of a single task with python operator',
    start_date=datetime(2025, 7, 10),
) as dag:

    task1 = PythonOperator(
        task_id='greet_task',
        python_callable=greet   # call python function in the task
    )
