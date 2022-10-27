from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from bacalhau.operators import BacalhauDockerRunJobOperator, BacalhauGetOperator, resolve_internal_path, BacalhauWasmRunJobOperator


with DAG('bacalhau-image-processing', start_date=datetime(2021, 1, 1)) as dag:

    op1 = BacalhauDockerRunJobOperator(
        task_id='op1',
        image='ghcr.io/bacalhau-project/examples/stable-diffusion-gpu:0.0.1',
        gpu='1',
        command='-- python main.py --o ./outputs --p "two people walking in a desert"',
    )

    get_1 = BacalhauGetOperator(
        task_id='get_job_data_1',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="op1", key="bacalhau_job_id") }}',
        output_dir='{{ dag.dag_id }}/op1/',
    )

    wasm = BacalhauWasmRunJobOperator(
        task_id='wasm',
        wasm='/Users/enricorotundo/winderresearch/ProtocolLabs/bacalhau-airflow-provider/rust-wasm/seam-carving/target/wasm32-wasi/release/my-program.wasm',
        entrypoint='_start',
        input_volumes=[
            '{{ task_instance.xcom_pull(task_ids="op1", key="cid") }}:inputs'
        ]
    )

    get_wasm = BacalhauGetOperator(
        task_id='get_job_data_wasm',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="wasm", key="bacalhau_job_id") }}',
        output_dir='{{ dag.dag_id }}/wasm/',
    )

    op2 = BacalhauDockerRunJobOperator(
        task_id='op2',
        image='ultralytics/yolov5:latest',
        gpu='1',
        command='-- python detect.py --weights ../../../datasets/yolov5s.pt --source /inputs/outputs/shrunk.png --project /outputs',
        inputs="{{ task_instance.xcom_pull(task_ids='wasm', key='cid') }}",
        input_volumes=[
            'bafybeicyuddgg4iliqzkx57twgshjluo2jtmlovovlx5lmgp5uoh3zrvpm:/datasets'
        ]
    )

    get_2 = BacalhauGetOperator(
        task_id='get_job_data_2',
        bacalhau_job_id='{{ task_instance.xcom_pull(task_ids="op2", key="bacalhau_job_id") }}',
        output_dir='{{ dag.dag_id }}/op2/',
    )

    op1 >> wasm >> op2
    op1 >> get_1
    wasm >> get_wasm
    op2 >> get_2

