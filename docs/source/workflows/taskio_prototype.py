from dataclasses import dataclass, asdict
from enum import Enum
import json
from typing import List, Dict

from prefect import task, flow


#############################
# Response struct definitions
#############################


class ResponseEnum(Enum):
    success = 0
    error = 1


@dataclass
class ETLResponseItem:
    primary_file_path: str
    status: ResponseEnum
    message: str
    title: str
    image_set: List


@dataclass
class ETLResponse:
    files: List[ETLResponseItem]


################################################################
# TaskIO is the data being passed down and updated as it passes
################################################################


@dataclass
class TaskIO:
    # output path is a file pointer returned by the most recent task
    # this is used as input by the downstream task
    output_path: str  # this should be pathlib.Path

    # file path are the keys to any taskio
    # they are unique
    # Sometimes, immediate upstream task may not be the right input
    # but the file_path (the original file) can be the needed one
    file_path: str = None  # set by the initializer

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
        prev_taskio: TaskIO = kwargs["taskio"]
        if prev_taskio.error:
            return prev_taskio

        try:
            new_taskio = func(**kwargs)
        except ValueError as e:
            # We are currently handling only ValueError.
            # So any other exception will cause pipeline to fail
            new_taskio = TaskIO(
                file_path=prev_taskio.file_path,
                output_path=None,
                error=f"{func.__name__} {str(e)}",
            )
        new_taskio.file_path = prev_taskio.file_path
        # if we want to save history of upstream tasks
        # new_taskio.upstream_history = prev_taskio.history
        # new_taskio.upstream_history[func.__name__] = new_taskio
        return new_taskio

    return wrapper


###############
# Sample tasks
###############


@task(name="task 1")  # names are needed, else all task names become "taskio_handler"
@taskio_handler
def task_a(taskio: TaskIO):
    input = taskio.output_path
    return TaskIO(output_path=input + "-> a")


@task(name="task 2")
@taskio_handler
def task_b(taskio: TaskIO):
    input = taskio.output_path
    output_path = input + "-> b"
    metadata = None
    # if '2' in taskio.file_path:
    #     raise RuntimeError("***I am not handled***")  # !!! This will halt the flow run !!!
    metadata = dict(
        imageType="B",
        params=f"param for {taskio.file_path} from B",
        path=output_path,
    )
    return TaskIO(output_path=output_path, data=dict(metadata=metadata))


@task(name="task 3")
@taskio_handler
def task_c(taskio: TaskIO):
    input = taskio.output_path
    # fails at 2
    output_path = input + "-> c"
    if "2" in input:
        raise ValueError("Fails at 2")
    metadata = dict(
        c=1,
        params=f"param for {taskio.file_path} from C",
        path=output_path,
    )
    return TaskIO(output_path=output_path, data=dict(metadata=metadata))


@task(name="task 4")
@taskio_handler
def task_d(taskio: TaskIO):
    input = taskio.output_path
    output_path = input + "-> d"
    metadata = dict(
        d=0,
        params=f"p for {taskio.file_path} from D",
        path=output_path,
    )
    return TaskIO(output_path=output_path, data=dict(metadata=metadata))


@task
def gen_response(inputs: List, taskios: List[TaskIO]):
    etl_items = {
        input.file_path: ETLResponseItem(
            primary_file_path=input.file_path,
            status=ResponseEnum.success.name,
            message=None,
            title=input.file_path,
            image_set=list(),
        )
        for input in inputs
    }

    for taskio in taskios:
        print(f"\n---\nTaskIO being processed for {taskio.file_path} >> {taskio} \n***")
        etl_item = etl_items[taskio.file_path]
        if taskio.error:
            etl_item.status = ResponseEnum.error.name
            etl_item.message = taskio.error
            etl_item.image_set = None
        else:
            if taskio.data:
                etl_item.image_set.append(taskio.data)

    resp = ETLResponse(files=list(etl_items.values()))
    return resp


@task
def task_fp(input):
    # generates the first stream
    return TaskIO(file_path=f"fp{input}", output_path=f"fp{input}")


@flow(log_prints=True)
def my_flow():
    inputs = "1 2 3".split()
    fp_outs = task_fp.map(inputs)
    a_outs = task_a.map(taskio=fp_outs)
    b_outs = task_b.map(taskio=a_outs)
    c_outs = task_c.map(taskio=a_outs)
    d_outs = task_d.map(taskio=c_outs)
    # technically each of the outputs that need to be stored
    # is passed through a func/task called get_asset
    response = gen_response.submit(fp_outs, [*b_outs, *c_outs, *d_outs])
    return response


if __name__ == "__main__":
    res = my_flow()
    print(json.dumps(asdict(res.result()), indent=2))
