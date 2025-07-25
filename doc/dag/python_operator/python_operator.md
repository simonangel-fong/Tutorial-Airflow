# Airflow - DAG: Python Operator

[Back](../../../README.md)

- [Airflow - DAG: Python Operator](#airflow---dag-python-operator)
  - [Python Operator](#python-operator)
  - [Passing Parameter](#passing-parameter)

## Python Operator

```py
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

# define py function to be executed


def greet():
    print("hello world")


with DAG(
    default_args=default_args,
    dag_id='python_operator_single_task',
    description='DAG of a single task with python operator',
    start_date=datetime(2025, 7, 10),
) as dag:

    task1 = PythonOperator(
        task_id='greet_task',
        python_callable=greet   # call python function in the task
    )

```

![python_operator_single_task](./pic/python_operator_single_task.png)

---

## Passing Parameter

```py
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data engineer',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

# define py function to be executed


def greet(msg: str):
    print(f"hello world, this is the message: {msg}")


with DAG(
    default_args=default_args,
    dag_id='python_operator_single_task_passing_parameter',
    description='DAG of a single task with python operator to pass parameter',
    start_date=datetime(2025, 7, 10),
) as dag:

    task1 = PythonOperator(
        task_id='greet_task',
        python_callable=greet,   # call python function in the task
        # setup parameters to pass.
        op_kwargs={'msg': "this is a new message."},
    )
```

![python_operator_single_task_passing_parameter](./pic/python_operator_single_task_passing_parameter.png)