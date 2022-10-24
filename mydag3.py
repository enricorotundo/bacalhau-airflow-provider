from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from bacalhau.operators import BacalhauDockerRunJobOperator, BacalhauGetOperator


with DAG('bacalhau-helloworld', start_date=datetime(2021, 1, 1)) as dag:
    # ipfs add ./1.txt

    op1 = BacalhauDockerRunJobOperator(
        task_id='job-hello-world',
        image='docker.io/winderresearch/bacalsummer:0.4',
        # command="cat /inputs/*.txt | awk '{n += $1}; END{print n}'",
        input_volumes=[
            'QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v:/inputs/1.txt',
            'QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v:/inputs/2.txt',
            'QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v:/inputs/3.txt',
            'QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v:/inputs/4.txt',
        ],
    )

    op2 = BacalhauDockerRunJobOperator(
        task_id='check-job',
        image='docker.io/winderresearch/bacalsummer:0.4',
        command='cat /inputs/prev-output/outputs/sum.txt',
        input_volumes=[
            '{{ task_instance.xcom_pull(task_ids="job-hello-world", key="cid") }}:/inputs/prev-output',
        ],
    )

    op3 = BacalhauGetOperator(
        task_id='get-job-data',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="check-job", key="bacalhau_job_id") }}',
        output_dir='/tmp/bacalhau',
    )

    op1 >> op2 >> op3

 