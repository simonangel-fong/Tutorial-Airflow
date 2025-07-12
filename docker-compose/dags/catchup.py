from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

with DAG(
    default_args=default_args,
    dag_id='catchup',
    description='DAG with single value xcom',
    start_date=datetime(2020, 1, 1),        # start from a past
    schedule='@daily',
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo hello world",
    )
