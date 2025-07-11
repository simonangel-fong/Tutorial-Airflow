from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

# define dag
@dag(
    dag_id="taskflow_api_single_output",
    default_args=default_args,
    start_date=None
)
def hello_world_etl():

    # define tasks
    @task()
    def set_base():
        return 2

    @task()
    def set_power():
        return 4

    @task()
    def display(base, power):
        print(f"Base:{base}, poser: {power}, the result is {base^power}")

    # define dependency
    base = set_base()
    power = set_power()
    display(base=base, power=power)

# create DAG instance 
myDAG = hello_world_etl()