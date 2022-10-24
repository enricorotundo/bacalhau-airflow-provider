from __future__ import annotations

import contextlib
import os
import signal
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir

from airflow.hooks.base import BaseHook

SubprocessResult = namedtuple('SubprocessResult', ['exit_code', 'output'])


class BacalhauHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module"""

    def __init__(self) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__()

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = 'utf-8',
        cwd: str | None = None,
    ) -> SubprocessResult:
        self.log.info('Tmp dir root location: \n %s', gettempdir())
        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix='airflowtmp'))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info('Running command: %s', command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info('Output:')
            line = ''
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b''):
                    line = raw_line.decode(output_encoding, errors='backslashreplace').rstrip()
                    self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info('Command exited with return code %s', self.sub_process.returncode)
            return_code: int = self.sub_process.returncode

        return SubprocessResult(exit_code=return_code, output=line)

    def send_sigterm(self):
        """Sends SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info('Sending SIGTERM signal to process group')
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)
