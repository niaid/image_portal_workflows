from prefect.tasks.shell import ShellTask
import prefect
from typing import List, Union, Optional
from prefect.utilities.tasks import defaults_from_attrs
from pathlib import Path
from em_workflows.utils import utils


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
        # to_log_local: Path = None,
    ) -> Optional[Union[str, List[str]]]:
        utils.log(command)

        # if to_echo:
        # utils.log(command)
        # logger = prefect.context.get("logger")
        # logger.info(to_echo)
        # if to_log_local and command:
        #    with open("{to_log_local.parent}/local_log.txt", "w") as _file:
        #        _file.write(command)
        if type(command) is list:
            command = command[0]
        super().run(command=command, env=env, helper_script=helper_script)
