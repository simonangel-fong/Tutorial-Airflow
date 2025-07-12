from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

with DAG(
    dag_id="postgresdb_create_insert",
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

    delete_exist = SQLExecuteQueryOperator(
        task_id="delete_exist",
        conn_id="postgres_conn",
        sql="""
            DELETE FROM dag_runs WHERE dt = '{{ds}}' AND dag_id = '{{dag.dag_id}}';
          """,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id="postgres_conn",
        sql="""
            INSERT INTO dag_runs (dt, dag_id) VALUES ('{{ds}}','{{dag.dag_id}}');
          """,
    )

    create_table >> delete_exist >> insert_data
