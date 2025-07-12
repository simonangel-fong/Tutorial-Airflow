import os
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="pgdb_import",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "csv"]
)
def import_csv():

    @task
    def load_csv_to_staging():
        file_path = "/opt/airflow/data/downloads/Ridership-2019-Q1.csv"

        # check if exist
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found.")

        df = pd.read_csv(file_path, dtype=str)  # Load all as str to avoid parse errors

        # Clean column name
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Connect PGDB
        hook = PostgresHook(postgres_conn_id="pgdb_conn")
        engine = hook.get_sqlalchemy_engine()

        # Load into the staging table
        df.to_sql("staging_tb", engine, if_exists="append", index=False)

    load_csv_to_staging()

dag = import_csv()