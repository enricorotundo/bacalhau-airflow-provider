import shutil

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.models import BaseOperator
from airflow.compat.functools import cached_property
from bacalhau.hooks import BacalhauHook


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
            command.append(self.command)
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
        print(f'Job ID: {job_id}')

        # store clientid in XCom
        client_id = self.subprocess_hook.run_command(
            command=[bash_path, '-c', f'bacalhau describe {str(result.output)} | yq \".ClientID\"'],
        )
        cli_id = str(client_id.output)
        context["ti"].xcom_push(key="client_id", value=cli_id)
        print(f'Client ID: {cli_id}')

        # store CID in XCom
        curl_cmd = f'curl --silent -X POST http://bootstrap.production.bacalhau.org:1234/results -H "Content-Type: application/json"'
        header = ' -d \'{"client_id":"' + cli_id + '","job_id":"' + job_id + '"}\''
        print(f'CURL command: {curl_cmd + header}')
        cid = self.subprocess_hook.run_command(
            command=[bash_path, '-c', curl_cmd + header + ' | jq \".results[0].Data.CID\"'],
        )
        cid_output = str(cid.output).replace('"', '')
        context["ti"].xcom_push(key="cid", value=cid_output)
        print(f'CID: {cid_output}')

        print(result.output)
        
        return result.output

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()


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
        curl_cmd = f'curl --silent -X POST http://bootstrap.production.bacalhau.org:1234/results -H "Content-Type: application/json"'
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
