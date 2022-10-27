from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from bacalhau.operators import BacalhauWasmRunJobOperator


with DAG('bacalhau-wasm', start_date=datetime(2021, 1, 1)) as dag:

    op1 = BacalhauWasmRunJobOperator(
        task_id='op1',
        wasm='bafybeiesfqvilkgpqt6psju7iytntq4fpsf6jwaoremt2ysjdhtmwuip4q',
        entrypoint='_start',
        input_volumes=[
            'bafybeifdpl6dw7atz6uealwjdklolvxrocavceorhb3eoq6y53cbtitbeu:/inputs'
        ]
    )

    op1


