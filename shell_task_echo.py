from prefect.tasks.shell import ShellTask
import prefect
from typing import List, Union, Optional
from prefect.utilities.tasks import defaults_from_attrs


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
    ) -> Optional[Union[str, List[str]]]:

        if to_echo:
            logger = prefect.context.get("logger")
            logger.info(to_echo)
        super().run(command=command, env=env, helper_script=helper_script)

