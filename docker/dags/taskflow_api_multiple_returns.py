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
    dag_id="taskflow_api_multiple_output",
    default_args=default_args,
    start_date=None
)
def hello_world_etl():

    # define tasks with multiple output
    @task(multiple_outputs=True)
    def set_base():
        return {
            "base1": 2,
            "base2": 3,
        }

    @task()
    def set_power():
        return 4

    @task()
    def display(base, power):
        print(
            f"Base01:{base['base1']}, base02: {base['base2']}, power: {power}")

    # define dependency
    base_dict = set_base()      # get the multiple output
    power = set_power()
    display(base=base_dict, power=power)


# create DAG instance
myDAG = hello_world_etl()
