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
    dag_id="multiple_tasks_dependency_operator",
    description="A DAG has multiple tasks using operator method.",
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

    task3 = BashOperator(
        task_id="3rd_task",
        bash_command="echo this the 3rd task."
    )

    task4 = BashOperator(
        task_id="4th_task",
        bash_command="echo this the 4th task."
    )

    task1 >> [task2, task3]

    task4 << [task2, task3]
