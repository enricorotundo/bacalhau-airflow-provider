from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from bacalhau.operators import BacalhauDockerRunJobOperator, BacalhauGetOperator


with DAG('bacalhau-helloworld', start_date=datetime(2021, 1, 1)) as dag:

    firs_sum = BacalhauDockerRunJobOperator(
        task_id='firs_sum',
        image='docker.io/winderresearch/bacalsummer:0.4',
        # command="cat /inputs/*.txt | awk '{n += $1}; END{print n}'",
        input_volumes=[
            'QmeBQ7h2MqEJfTJdrM7yeEYyFcdhM1LXHh2esGHTNWmq7T:/inputs/00.txt',
            'QmRatDD9i6LxmhhaboANXk5Z41QT28Yr27dNUWdEPxkVPp:/inputs/01.txt',
        ],
    )


    second_sum = BacalhauDockerRunJobOperator(
        task_id='second_sum',
        image='docker.io/winderresearch/bacalsummer:0.4',
        input_volumes=[
            '{{ task_instance.xcom_pull(task_ids="firs_sum", key="cid") }}:/inputs/001.txt',
            'QmZt8BYk35FM6qSTZiti9YaJFtKaDcuetoUVArT141GUtg:/inputs/02.txt',
            'QmasQjyyja5xK9S7qSKtfkxT48agxKmeiPKzGpjSusCabD:/inputs/03.txt',
        ],
    )

    check_results = BacalhauDockerRunJobOperator(
        task_id='check_results',
        image='ubuntu:latest',
        command='cat /inputs/prev-output/outputs/sum.txt',
        input_volumes=[
            '{{ task_instance.xcom_pull(task_ids="second_sum", key="cid") }}:/inputs/prev-output',
        ],
    )

    clean_dir = BashOperator(
        task_id='clean_dir',
        bash_command='rm -rf /tmp/bacalhau && mkdir -p /tmp/bacalhau',
    )

    get_job_data = BacalhauGetOperator(
        task_id='get_job_data',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="check_results", key="bacalhau_job_id") }}',
        output_dir='/tmp/bacalhau',
    )

    firs_sum >> second_sum >> check_results >> clean_dir >> get_job_data
