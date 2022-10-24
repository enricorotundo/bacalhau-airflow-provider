from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from custom_operator.bacalhau_operator import BacalhauOperator


with DAG('first-bacalhau-dag-2', start_date=datetime(2021, 1, 1)) as dag:
    op1 = BacalhauOperator(
        task_id='submit-a-job',
        bacalhau_command='docker run ubuntu date',
    )

    op2 = BacalhauOperator(
        task_id='get-job-data',
        bacalhau_command='version',
    )

    op1 >> op2