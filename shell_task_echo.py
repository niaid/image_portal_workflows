from prefect.tasks.shell import ShellTask
import prefect
from typing import List, Union, Optional
from prefect.utilities.tasks import defaults_from_attrs
from pathlib import Path


def echo(command):
    return f"echo {command}"


class ShellTaskEcho(ShellTask):
    @defaults_from_attrs("command", "env", "helper_script")
    def run(
        self,
        command: str = None,
        env: dict = None,
        helper_script: str = None,
        to_echo: str = None,
        to_log_local: Path = None,
    ) -> Optional[Union[str, List[str]]]:

        if to_echo:
            logger = prefect.context.get("logger")
            logger.info(to_echo)
        if to_log_local and command:
            with open("{to_log_local.parent}/local_log.txt", "w") as _file:
                _file.write(command)
        super().run(command=command, env=env, helper_script=helper_script)

