from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash_operator import BashOperator
from airflow.models.taskinstance import TaskInstance


class BacalhauOperator(BashOperator):

    @apply_defaults
    def __init__(self,
                bacalhau_command='',
                bash_command='',
                 *args,
                 **kwargs):

        if bacalhau_command.startswith("docker run"):
            bash_command = f"docker run --id-only --wait {bacalhau_command.replace('docker run', '')}"
        elif bacalhau_command.startswith("get"):
            bash_command = f"get --download-timeout-secs 10 --output-dir /tmp/enrico/ {bacalhau_command.replace('get', '')}"
        elif bacalhau_command.startswith("version"):
            bash_command = f"version --output json"
        else:
            raise NotImplementedError(f"Command not implemented by operator")

        

        super().__init__(bash_command='bacalhau ' + bash_command, *args, **kwargs)

    def execute(self, context):
        print(self.task_id)
        print(context['ti'])
        print(context.keys())
        print(context['conf'])
        super().execute(context)
        print("done")
        return 1