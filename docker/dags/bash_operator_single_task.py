from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "Data engineer",
    "retries": "5",
    "retry_delay": timedelta(minutes=2),
    "schedule_interval": '@daily',
}

with DAG(
    dag_id="Single_Task_DAG",
    description="A DAG has a single task.",
    start_date=datetime(2025, 7, 10),
    default_args=default_args,
) as dag:
    task1 = BashOperator(
        task_id="1st_task",
        bash_command="echo this the 1st task."
    )
