import shutil
import os
from typing import List

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from airflow.models import BaseOperator, TaskInstance
from airflow.compat.functools import cached_property
from attr import attr
from openlineage.airflow.extractors.base import OperatorLineage
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Dataset

from bacalhau.hooks import BacalhauHook

wasm_path = '/Users/enricorotundo/winderresearch/ProtocolLabs/bacalhau-airflow-provider/rust-wasm/seam-carving/target/wasm32-wasi/release/my-program.wasm'

def resolve_internal_path(task):
    if len(task.output_volumes) == 0:
        return 'outputs'
    else:
        return task.output_volumes

class BacalhauDockerRunJobOperator(BaseOperator):
    """
    This operator is a wrapper around the `bacalhau docker run` command line tool.
    It allows you to run a Bacalhau job in a docker container.
    """
    ui_color = "#FFF9DA"
    ui_fgcolor = "#04206F"
    custom_operator_name = "BacalhauDockerRun"
    template_fields = (
        'image',
        'command',
        'inputs',
        'input_volumes',
    )

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running a bacalhau command"""
        return BacalhauHook()

    def __init__(self,
        image,
        command='',
        concurrency = 1,
        dry_run = False,
        env = [],
        gpu = '',
        input_urls = [],
        input_volumes = [],
        inputs = "",
        output_volumes = [],
        publisher = 'estuary',
        workdir = '',
        **kwargs) -> None:
        super().__init__(**kwargs)
        # On start properties
        self.image = image
        self.command = command
        self.concurrency = concurrency
        self.dry_run = dry_run
        self.env = env
        self.gpu = gpu
        self.input_urls = input_urls
        self.input_volumes = input_volumes
        self.inputs = inputs
        self.output_volumes = output_volumes
        self.publisher = publisher
        self.workdir = workdir
        # On complete properties
        self.bacalhau_job_id = ""
        self.client_id = ""
        self.cid_ouput = ""

    def execute(self, context: Context):
        bash_path = shutil.which("bash") or "bash"
        command = ['bacalhau', 'docker run', '--id-only', '--wait', '--publisher ipfs']

        # build flags
        if self.concurrency != 1:
            command.append(f'--concurrency {self.concurrency}')
        if self.dry_run:
            command.append('--dry-run')
        if len(self.env) > 0:
            for envar in self.env:
                command.append(f'--env {envar}')
        if len(self.gpu) > 0:
            command.append(f'--gpu {self.gpu}')
        if len(self.input_urls) > 0:
            for url in self.input_urls:
                command.append(f'--input-urls {url}')
        if len(self.input_volumes) > 0:
            for volume in self.input_volumes:
                command.append(f'--input-volumes {volume}')
        if self.inputs != "":
            command.append(f'--inputs {self.inputs}')
        if len(self.output_volumes) > 0:
            for volume in self.output_volumes:
                command.append(f'--output-volumes {volume}')
        if self.publisher != 'estuary':
            command.append(f'--publisher {self.publisher}')
        if len(self.workdir) > 0:
            command.append(f'--workdir {self.workdir}')

        command.append(self.image)
        if len(self.command) > 0:
            command.append('-- ' + self.command)
        print(f'Final command: {command}')

        # execute command
        result = self.subprocess_hook.run_command(
            command=[bash_path, '-c', ' '.join(command)],
        )
        if result.exit_code != 0:
            raise AirflowException(f'Bash command failed. The command returned a non-zero exit code {result.exit_code}.')

        # store jobid in XCom
        job_id = str(result.output)
        context["ti"].xcom_push(key="bacalhau_job_id", value=job_id)
        self.bacalhau_job_id = job_id
        print(f'Job ID: {job_id}')

        # store clientid in XCom
        client_id = self.subprocess_hook.run_command(
            command=[bash_path, '-c', f'bacalhau describe {str(result.output)} | yq \".ClientID\"'],
        )
        cli_id = str(client_id.output)
        context["ti"].xcom_push(key="client_id", value=cli_id)
        self.client_id = cli_id
        print(f'Client ID: {cli_id}')

        # store CID in XCom
        curl_cmd = f'curl --silent -X POST {os.getenv("BACALHAU_API_HOST")}:1234/results -H "Content-Type: application/json"'
        header = ' -d \'{"client_id":"' + cli_id + '","job_id":"' + job_id + '"}\''
        print(f'CURL command: {curl_cmd + header}')
        cid = self.subprocess_hook.run_command(
            command=[bash_path, '-c', curl_cmd + header + ' | jq \".results[0].Data.CID\"'],
        )
        cid_output = str(cid.output).replace('"', '')
        context["ti"].xcom_push(key="cid", value=cid_output)
        self.cid_ouput = cid_output
        print(f'CID: {cid_output}')

        print(result.output)

        return result.output

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()

    # get_openlineage_facets_on_start() is run by Openlineage/Marquez before the execute() funciton is run, allowing
    # to collect metadata before the execution of the task.
    # Implementation details can be found in Openlineage doc: https://openlineage.io/docs/integrations/airflow/operator#implementation
    # TODO this peace of code has not been tested and should be refactored before being used
    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=[
                Dataset(
                    namespace=f'{os.getenv("BACALHAU_API_HOST")}:1234',
                    name="inputs",
                    facets={
                        "command":  self.command,
                        "concurrency":  self.concurrency,
                        "dry_run":  self.dry_run,
                        "env":  self.env,
                        "gpu":  self.gpu,
                        "input_urls":  self.input_urls,
                        "input_volumes":  self.input_volumes,
                        "inputs":  self.inputs,
                        "output_volumes":  self.output_volumes,
                        "publisher":  self.publisher,
                        "workdir":  self.workdir
                    }
                )
            ],
            output=[],
            run_facets={},
            job_facets={},
        )

    # get_openlineage_facets_on_complete() is run by Openlineage/Marquez after the execute() funciton is run, allowing
    # to collect metadata after the execution of the task.
    # TODO this peace of code has not been tested and should be refactored before being used
    def get_openlineage_facets_on_complete(self, task_instance: TaskInstance) -> OperatorLineage:
        return OperatorLineage(
            inputs=[
                Dataset(
                    namespace=f'{os.getenv("BACALHAU_API_HOST")}:1234',
                    name="inputs",
                    facets={
                        "command": self.command,
                        "concurrency": self.concurrency,
                        "dry_run": self.dry_run,
                        "env": self.env,
                        "gpu": self.gpu,
                        "input_urls": self.input_urls,
                        "input_volumes": self.input_volumes,
                        "inputs": self.inputs,
                        "output_volumes": self.output_volumes,
                        "publisher": self.publisher,
                        "workdir": self.workdir
                    }
                )
            ],
            outputs=[
                Dataset(
                    namespace=f'{os.getenv("BACALHAU_API_HOST")}:1234',
                    name="outputs",
                    facets={
                        "cid_output": self.cid_ouput,
                    }
                )
            ],
            run_facets={
                "bacalhau": BacalauRunFacet(self.client_id)
            },
            job_facets={
                "bacalhau": BacalauJobFacet(self.bacalhau_job_id)
            }
        )

@attr.s
class BacalauRunFacet(BaseFacet):
    client_id: str = attr.ib()
    _additional_skip_redact: List[str] = ['client_id']
    def __init__(self, client_id):
        super().__init__()
        self.client_id = client_id

@attr.s
class BacalauJobFacet(BaseFacet):
    job_id: str = attr.ib()
    _additional_skip_redact: List[str] = ['job_id']
    def __init__(self, job_id):
        super().__init__()
        self.job_id = job_id


class BacalhauWasmRunJobOperator(BaseOperator):
    """
    This operator is a wrapper around the `bacalhau wasm run` command line tool.
    It allows you to run a Bacalhau job in a docker container.
    """
    ui_color = "#FFF9DA"
    ui_fgcolor = "#04206F"
    custom_operator_name = "BacalhauWasm"
    template_fields = (
        'input_volumes',
    )
    @cached_property
    def subprocess_hook(self):
        """Returns hook for running a bacalhau command"""
        return BacalhauHook()

    def __init__(self,
        command='',
        wasm='',
        entrypoint='',
        input_volumes = [],
        **kwargs) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.input_volumes = input_volumes
        self.wasm = wasm
        self.entrypoint = entrypoint

    def execute(self, context: Context):
        bash_path = shutil.which("bash") or "bash"
        command = ['bacalhau', 'wasm run', '--id-only', '--publisher ipfs']

        command.append(self.wasm)
        command.append(self.entrypoint)

        if len(self.input_volumes) > 0:
            for volume in self.input_volumes:
                command.append(f'--input-volumes {volume}')

        # execute command
        result = self.subprocess_hook.run_command(
            command=[bash_path, '-c', ' '.join(command)],
        )
        if result.exit_code != 0:
            raise AirflowException(f'Bash command failed. The command returned a non-zero exit code {result.exit_code}.')

        # store jobid in XCom
        job_id = str(result.output)
        context["ti"].xcom_push(key="bacalhau_job_id", value=job_id)

        # store clientid in XCom
        client_id = self.subprocess_hook.run_command(
            command=[bash_path, '-c', f'bacalhau describe {str(result.output)} | yq \".ClientID\"'],
        )
        cli_id = str(client_id.output)
        context["ti"].xcom_push(key="client_id", value=cli_id)
        print(f'Client ID: {cli_id}')

        # store CID in XCom
        curl_cmd = f'curl --silent -X POST {os.getenv("BACALHAU_API_HOST")}:1234/results -H "Content-Type: application/json"'
        header = ' -d \'{"client_id":"' + cli_id + '","job_id":"' + job_id + '"}\''
        print(f'CURL command: {curl_cmd + header}')
        cid = self.subprocess_hook.run_command(
            command=[bash_path, '-c', curl_cmd + header + ' | jq \".results[0].Data.CID\"'],
        )
        cid_output = str(cid.output).replace('"', '')
        context["ti"].xcom_push(key="cid", value=cid_output)

        print(result.output)
        return result.output

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()

class BacalhauGetOperator(BaseOperator):
    """
    This operator is a wrapper around the ``bacalhau get`` command line tool.
    It allows you to download the artifacts of a Bacalhau job to a local directory.
    """
    template_fields = (
        'bacalhau_job_id',
        'output_dir',
    )

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running a bacalhau command"""
        return BacalhauHook()

    def __init__(self,
        bacalhau_job_id,
        output_dir,
        download_timeout_secs = 300,
        **kwargs) -> None:
        super().__init__(**kwargs)
        self.bacalhau_job_id = bacalhau_job_id
        self.download_timeout_secs = download_timeout_secs
        self.output_dir = '/tmp/bacalhau/' + output_dir

    def execute(self, context: Context):
        bash_path = shutil.which("bash") or "bash"
        command = [f'rm -rf {self.output_dir} && mkdir -p {self.output_dir} &&', 'bacalhau', 'get']

        if self.download_timeout_secs != 300:
            command.append(f'--download-timeout-secs {self.download_timeout_secs}')
        if self.output_dir != '.':
            command.append(f'--output-dir {self.output_dir}')

        command.append(self.bacalhau_job_id)

        print(f'Final command: {command}')
        result = self.subprocess_hook.run_command(
            command=[bash_path, '-c', ' '.join(command)],
        )

        return result.output

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()
