# Airflow - Fundamental

[Back](../../README.md)

---

##

- `Airflow`:

  - a workflow management platform written in Python
  - “workflows as code” approach

- Workflow:

  - a sequence of tasks
  - defined as DAG in airflow

- `Directed Acyclic Graph(DAG)`

  - a graph where connections between nodes (also called vertices) have a direction (directed) and do not form any closed loops (acyclic).
  - a task

- A task in airflow

  - a unit of work
  - written in Python
  - has dependence with other task

- Operator
  - used to define a specific task to be executed
- Common types:
  - BashOperator
  - PythonOperator

---

- `Execution Date`
  - the date when a DAG runs
- `Task Instance` = `Operator` + Execution Data
- `Dag Run` = `DAG` + Execution Data

---

## Task Lifecycle

- 11 kinds of stages
- `no_status`: scheduler creates an empty task instance
  - scheduler
  - `scheduled`: scheduler determin a task instance needs to be run
    - Executor
      - `queued`
        - Worker
          - `running`: task under execution
            - `Success`: task complete flawlessly
            - `Failed`: task fails
              - `up for retry`: task re-run when under maximum retry limit
            - `Shutdown`: task has been aborted
              - `up for retry`: task re-run when under maximum retry limit
  - `upstream failed`: an upstream task failed
  - `skipped`: a tasked is skipped.

---
