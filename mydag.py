from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from custom_operator.bacalhau_operator import BacalhauOperator


with DAG('first-bacalhau-dag', start_date=datetime(2021, 1, 1)) as dag:
    start = EmptyOperator(
        task_id='start',
    )

    op1 = BashOperator(
        task_id='submit-a-job',
        bash_command='echo hello {{ ti.xcom_push(key="jobid", value="$(bacalhau docker run --id-only --wait ubuntu date)") }}',
    )

    op2 = BashOperator(
        task_id='get-job-data',
        bash_command='bacalhau get --download-timeout-secs 10 --output-dir /tmp/enrico/ {{ ti.xcom_pull(key="jobid") }}',
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> op1 >> op2 >> end
