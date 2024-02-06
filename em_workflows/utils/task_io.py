from dataclasses import dataclass
from pathlib import Path
from prefect import task
from typing import List, Dict

from em_workflows.file_path import FilePath


@dataclass
class TaskIO:
    # output path is a file pointer returned by the most recent task
    # this is used as input by the downstream task
    output_path: Path  # this should be pathlib.Path

    # file path are the keys to any taskio
    # they are unique
    # Sometimes, immediate upstream task may not be the right input
    # but the file_path (the original file) can be the needed one
    file_path: FilePath = None  # set by the initializer

    # error reflects the known error of the oldest task upstream
    error: str = None

    # along with output_path, some taskios generate data to pass to the users
    data: Dict = None

    # Upstream history could be added to store other intermediate results
    # produced by upstream tasks (which may or may not be necessary downstream)
    # upstream_history: Dict


def taskio_handler(func):
    """
    Takes in taskio and passes into the task if it is valid
    If the function raises an error, annotates error (into new_taskio) and passes downstream
    If the function runs fine, passes the resulting taskio (new_taskio) as is

    If the upstream has error, passes it downstream as is
        does not pass into downstream tasks
    """

    def wrapper(taskio):
        prev_taskio: TaskIO = taskio

        if prev_taskio.error:
            return prev_taskio

        try:
            new_taskio = func(taskio)
        except RuntimeError as e:
            # We are raising errors that are instance of RuntimeError
            new_taskio = TaskIO(
                file_path=prev_taskio.file_path,
                output_path=None,
                error=f"{func.__name__} {str(e)}",
            )
        except Exception:
            # handle any other exception
            new_taskio = TaskIO(
                file_path=prev_taskio.file_path,
                output_path=None,
                error="Something went wrong!",
            )
        new_taskio.file_path = prev_taskio.file_path
        # if we want to save history of upstream tasks
        # new_taskio.upstream_history = prev_taskio.history
        # new_taskio.upstream_history[func.__name__] = new_taskio

        return new_taskio

    return wrapper


@task
def gen_response(fps: List[TaskIO], assets: List[TaskIO]):
    # turning a list to dict with primary filepath as the key
    etl_items = {
        etl_item.file_path.fp_in: etl_item.file_path.gen_prim_fp_elt()
        for etl_item in fps
    }

    for asset in assets:
        print(f"\n---\nTaskIO being processed for {asset.file_path.fp_in}\n\n***")
        etl_item = etl_items[asset.file_path.fp_in]

        # if error is already registered from previous asset... ignore
        if etl_item["status"] == "error":
            continue

        if asset.error:
            etl_item["status"] = "error"
            etl_item["message"] = asset.error
            etl_item["imageSet"] = None
        else:
            if isinstance(asset.data, list):
                etl_item["imageSet"][0]["assets"].extend(asset.data)
            elif isinstance(asset.data, dict):
                etl_item["imageSet"][0]["assets"].append(asset.data)

    resp = list(etl_items.values())
    return resp


@task
def gen_taskio(share_name: str, input_dir: Path, input_fps: List[Path]) -> TaskIO:
    result = list()
    for fp_in in input_fps:
        file_path = FilePath(share_name=share_name, input_dir=input_dir, fp_in=fp_in)
        result.append(TaskIO(file_path=file_path, output_path=file_path))
    return result
