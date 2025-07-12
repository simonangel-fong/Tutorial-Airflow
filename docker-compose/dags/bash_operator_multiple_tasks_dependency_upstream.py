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
    dag_id="multiple_tasks_dependency_upstream",
    description="A DAG has multiple tasks using upstream method.",
    start_date=datetime(2025, 7, 10),
    default_args=default_args,
) as dag:
    task1 = BashOperator(
        task_id="1st_task",
        bash_command="echo this the 1st task."
    )

    task2 = BashOperator(
        task_id="2nd_task",
        bash_command="echo this the 2nd task."
    )

    task2.set_upstream(task1)
