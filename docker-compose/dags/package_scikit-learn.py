from datetime import datetime, timedelta
from airflow import DAG, task
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}


def get_sklearn():
    import sklearn
    print(f"scikit-learn version: {sklearn.__version__}")

def get_matplotlib():
    import matplotlib
    print(f"scikit-learn version: {matplotlib.__version__}")

with DAG(
    default_args=default_args,
    dag_id='python_package_scikit_learn',
    description='DAG to verify the installed package',
    start_date=None
) as dag:
    get_sklearn = PythonOperator(
        task_id="get_sklearn",
        python_callable=get_sklearn
    )

    get_matplotlib = PythonOperator(
        task_id="get_matplotlib",
        python_callable=get_matplotlib
    )

    get_sklearn >> get_matplotlib
