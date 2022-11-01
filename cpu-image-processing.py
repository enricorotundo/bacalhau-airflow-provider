from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from bacalhau.operators import BacalhauDockerRunJobOperator, BacalhauGetOperator, BacalhauWasmRunJobOperator, wasm_path


with DAG('image-processing-pipeline-demo', start_date=datetime(2021, 1, 1)) as dag:

    wasm = BacalhauWasmRunJobOperator(
        task_id='wasm',
        wasm=wasm_path,
        entrypoint='_start',
        input_volumes=[
            'bafybeiedc5botuikjch3gqsvjhjgd3hq4uc7nvb2w6q3stnqesjdddms3i:inputs'
        ]
    )

    object_detection = BacalhauDockerRunJobOperator(
        task_id='object_detection',
        image='ultralytics/yolov5:latest',
        command='python detect.py --weights ../../../datasets/yolov5s.pt --source /inputs/outputs/shrunk.png --project /outputs',
        inputs="{{ task_instance.xcom_pull(task_ids='wasm', key='cid') }}",
        input_volumes=[
            'bafybeicyuddgg4iliqzkx57twgshjluo2jtmlovovlx5lmgp5uoh3zrvpm:/datasets'
        ]
    )

    get_wasm = BacalhauGetOperator(
        task_id='get_job_data_wasm',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="wasm", key="bacalhau_job_id") }}',
        output_dir='{{ dag.dag_id }}/wasm/',
    )

    get_object_detection = BacalhauGetOperator(
        task_id='get_job_data_2',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="object_detection", key="bacalhau_job_id") }}',
        output_dir='{{ dag.dag_id }}/op2/',
    )

    wasm >> object_detection
    wasm >> get_wasm 
    object_detection >> get_object_detection
