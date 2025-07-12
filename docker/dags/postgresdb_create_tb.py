from datetime import datetime, timedelta

from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

with DAG(
    dag_id="postgres_create_tb",
    default_args=default_args,
    start_date=None,
    schedule='@daily',
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
          """,
    )
