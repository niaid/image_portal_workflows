from dataclasses import dataclass
from pathlib import Path
from prefect import task
from typing import List, Dict

from em_workflows.config import Config
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

    def wrapper(**kwargs):
        assert (
            "taskio" in kwargs
        ), "Task functions must have `taskio` keyword argument in their definition to use `taskio_handler` definition."
        prev_taskio: TaskIO = kwargs["taskio"]
        if prev_taskio.error:
            return prev_taskio

        try:
            new_taskio = func(**kwargs)
        except RuntimeError as e:
            # We are currently handling only ValueError.
            # So any other exception will cause pipeline to fail
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
def gen_response(fps: List[TaskIO], taskios: List[TaskIO]):
    # turning a list to dict with primary filepath as the key
    etl_items = {
        etl_item.file_path.fp_in: etl_item.file_path.gen_prim_fp_elt()
        for etl_item in fps
    }

    for taskio in taskios:
        print(f"\n---\nTaskIO being processed for {taskio.file_path.fp_in}\n\n***")
        etl_item = etl_items[taskio.file_path.fp_in]
        # if error is already registered... ignore
        if etl_item["status"] == "error":
            continue

        if taskio.error:
            etl_item["status"] = "error"
            etl_item["message"] = taskio.error
            etl_item["imageSet"] = None
        else:
            if isinstance(taskio.data, list):
                etl_item["imageSet"][0]["assets"].extend(taskio.data)
            elif isinstance(taskio.data, dict):
                etl_item["imageSet"][0]["assets"].append(taskio.data)

    resp = list(etl_items.values())
    return resp


@task(
    # persisting to retrieve again in hooks
    persist_result=True,
    result_storage=Config.local_storage,
    result_serializer=Config.pickle_serializer,
    result_storage_key="{flow_run.id}__gen_fps",
)
def gen_taskio(share_name: str, input_dir: Path, fp_in: Path) -> TaskIO:
    file_path = FilePath(share_name=share_name, input_dir=input_dir, fp_in=fp_in)
    return TaskIO(file_path=file_path, output_path=file_path)


@task
def gen_prim_fps(taskio: TaskIO) -> Dict:
    base_elt = taskio.file_path.gen_prim_fp_elt()
    return base_elt
