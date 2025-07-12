from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}


# define py function to be executed

def set_message():
    return "test message"


def display_message(**context):
    msg = context["ti"].xcom_pull(task_ids="task_set_message")
    if msg is not None:
        print(f"this is message:{msg}")
    else:
        print(f"Message is none")


with DAG(
    default_args=default_args,
    dag_id='python_operator_xcom_single_value',
    description='DAG with single value xcom',
    start_date=datetime(2025, 7, 10),
    schedule='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='task_set_message',
        python_callable=set_message,
    )

    # task to get parameter from xcom
    task2 = PythonOperator(
        task_id='task_display_message',
        python_callable=display_message,
    )

    task1 >> task2
