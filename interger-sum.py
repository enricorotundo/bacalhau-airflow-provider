from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from bacalhau.operators import BacalhauDockerRunJobOperator, BacalhauGetOperator, resolve_internal_path


with DAG('bacalhau-integer-sum', start_date=datetime(2021, 1, 1)) as dag:

    number_generator_1 = BashOperator(
        task_id='number_generator_1',
        bash_command='jot -r 1 0 10000  | ipfs'
                     ' add --quiet --stdin-name "${EPOCHREALTIME/./}.txt"',
    )

    number_generator_2 = BashOperator(
        task_id='number_generator_2',
        bash_command='jot -r 1 0 10000  | ipfs'
                    #  '{{ dag_run.conf["ipfs_address"] }}'
                     ' add --quiet --stdin-name "${EPOCHREALTIME/./}.txt"',
    )
    
    first_sum = BacalhauDockerRunJobOperator(
        task_id='first_sum',
        image='docker.io/winderresearch/bacalsummer:0.4',
        input_volumes=[
            "{{ task_instance.xcom_pull(task_ids='number_generator_1', key='return_value') }}:/inputs/01.txt",
            "{{ task_instance.xcom_pull(task_ids='number_generator_2', key='return_value') }}:/inputs/02.txt",
        ],
    )

    number_generator_3 = BashOperator(
        task_id='number_generator_3',
        bash_command='jot -r 1 0 10000  | ipfs'
                     ' add --quiet --stdin-name "${EPOCHREALTIME/./}.txt"',
    )

    number_generator_4 = BashOperator(
        task_id='number_generator_4',
        bash_command='jot -r 1 0 10000  | ipfs'
                     ' add --quiet --stdin-name "${EPOCHREALTIME/./}.txt"',
    )

    second_sum = BacalhauDockerRunJobOperator(
        task_id='second_sum',
        image='docker.io/winderresearch/bacalsummer:0.4',
        input_volumes=[
            "{{ task_instance.xcom_pull(task_ids='number_generator_3', key='return_value') }}:/inputs/03.txt",
            "{{ task_instance.xcom_pull(task_ids='number_generator_4', key='return_value') }}:/inputs/04.txt",
        ],
    )

    overall_sum = BacalhauDockerRunJobOperator(
        task_id='overall_sum',
        image='docker.io/winderresearch/bacalsummer:0.4',
        input_volumes=[
            '{{ task_instance.xcom_pull(task_ids="first_sum", key="cid") }}:/inputs/00.txt',
            '{{ task_instance.xcom_pull(task_ids="second_sum", key="cid") }}:/inputs/01.txt',
        ],
    )

    sub_path = resolve_internal_path(overall_sum)
    check_results = BacalhauDockerRunJobOperator(
        task_id='check_results',
        image='ubuntu:latest',
        command=f'cat /inputs/{sub_path}/sum.txt',
        inputs='{{ task_instance.xcom_pull(task_ids="second_sum", key="cid") }}',
    )

    get_job_data = BacalhauGetOperator(
        task_id='get_job_data',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="check_results", key="bacalhau_job_id") }}',
        output_dir='{{ dag.dag_id }}',
    )

    [number_generator_1, number_generator_2] >> first_sum
    [number_generator_3, number_generator_4] >> second_sum
    [first_sum, second_sum] >> overall_sum >> check_results >> get_job_data
