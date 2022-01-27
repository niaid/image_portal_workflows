import requests
import os
import shutil
import json
import prefect
from typing import List, Dict
from pathlib import Path
from prefect import task, context
from prefect import Flow, task, context
from prefect.engine.state import State
from prefect.engine import signals

from image_portal_workflows.config import Config

logger = context.get("logger")


def _add_outputs(
    dname: str, files: List[Dict], outputs: List[Path], _type: str
) -> List[Dict]:
    """
    converts a list of Paths to a data structure used to create JSON for
    the callback
    """
    for i, elt in enumerate(files):
        elt["assets"].append({"type": _type, "path": dname + outputs[i].as_posix()})
    return files


@task
def list_files(input_dir: Path, exts: List[str]) -> List[Path]:
    """
    workflows have to find which inputs to run on, inputs are defined as all
    files within a directory.
    """
    _files = list()
    for ext in exts:
        _files.extend(input_dir.glob(f"*.{ext}"))
    _file_names = [Path(_file.name) for _file in _files]
    logger.info("Found files:")
    logger.info(_file_names)
    #    if not _files:
    #        raise ValueError(f"{input_dir} contains no files with extension {ext}")
    return _file_names


# class Job:
#    def __init__(self, input_dir):
#        self.output_dir = Path(Config.assets_dir + input_dir)
#        self.input_dir = Path(Config.proj_dir + input_dir)
# @task
# def init_job(input_dir: Path) -> Job:
#    """not clear if this class is needed - seems to only be used to house input_dir
#    ATM."""
#    return Job(input_dir=input_dir)


@task
def gen_output_fname(input_fp: Path, output_ext) -> Path:
    """
    Each file is generated using the input file name, without extension,
    with a new extension."""
    output_fp = Path(f"{input_fp.stem}{output_ext}")
    logger.info(f"input: {input_fp} output: {output_fp}")
    return output_fp


@task
def run_single_file(input_fps: List[Path], fp_to_check: str) -> List[Path]:
    """
    Workflows can be run on single files, if the file_name param is used.
    This function will limit the list of inputs to only that file_name (if
    provided), and check the file exists, if so will return as Path, else
    raise exception."""
    if fp_to_check is None:
        return input_fps
    for _fp in input_fps:
        if _fp.name == fp_to_check:
            return [Path(fp_to_check)]
    raise signals.FAIL(f"Expecting file: {fp_to_check}, not found in input_dir")


def _gen_callback_file_list(dname: str, inputs: List[Path]) -> List[Dict]:
    """
    converts a list of Paths to a datastructure used to create JSON for
    the callback
    """
    if not dname.endswith("/"):
        dname = dname + "/"
    files = list()
    for _file in inputs:
        elt = {
            "primaryFilePath": dname + _file.as_posix(),
            "title": _file.stem,
            "assets": list(),
        }
        files.append(elt)
    return files


def notify_api_running(flow: Flow, old_state, new_state) -> State:
    """
    tells API the workflow has started to run.
    """
    if new_state.is_running():
        callback_url = prefect.context.get("callback_url")
        token = prefect.context.get("token")
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }
        response = requests.post(
            callback_url, headers=headers, data=json.dumps({"status": "running"})
        )
        logger.info(response.text)
    return new_state


def notify_api_completion(flow: Flow, old_state, new_state) -> State:
    """
    Prefect workflows transition from State to State, see:
    https://docs.prefect.io/core/concepts/states.html#overview.
    This method checks if the State being transitioned into is an is_finished state.
    If it is, a notification is sent stating the workflow is finished.
    Is a static method because signiture much conform as above, see:
    https://docs.prefect.io/core/concepts/notifications.html#state-handlers

    """
    if new_state.is_finished():
        status = ""
        if new_state.is_successful():
            status = "success"
        else:
            status = "fail"
        callback_url = prefect.context.parameters.get("callback_url")
        token = prefect.context.parameters.get("token")
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }
        response = requests.post(
            callback_url, headers=headers, data=json.dumps({"status": status})
        )
        logger.info(response.text)
    return new_state


@task
def copy_inputs_to_outputs_dir(input_dir_fp: Path, output_dir_fp: Path):
    """
    inputs are found in {nfs_dir}/Projects/Lab/PI/Proj_name/Session_name/Sample_name/
    outputs are placed in {nfs_dir}/Assets/Lab/PI/Proj_name/Session_name/Sample_name/
    I'm going to copy inputs over, and process them in place.
    """
    fps = list()
    for ext in Config.two_d_input_exts:
        fps = input_dir_fp.glob(f"*.{ext}")
        for fp in fps:
            full_fp = f"{fp}"
            logger.info(f"coping {full_fp} to {output_dir_fp}")
            shutil.copy(full_fp, output_dir_fp)


@task
def gen_output_dir(input_dir: str) -> Path:
    """The output directory, ie the place outputs are written to mirrors
    input_dir, except rather than the path getting rooted "{nfs_dir}/Projects/...", outputs
    are rooted in "{nfs_dir}/Assets/...".
    """
    output_path = Path(Config.assets_dir + input_dir)
    logger.info(f"Output path is {output_path}")
    os.makedirs(output_path.as_posix(), exist_ok=True)
    return output_path


@task
def get_input_dir(input_dir: str) -> Path:
    """Does nothing more than concat the POSTed input file path to the
    mount point.
    """
    input_path = Path(Config.proj_dir + input_dir)
    logger.info(f"Input path is {input_path}")
    return input_path


@task
def clean_up_outputs_dir(assets_dir: Path, to_keep: List[Dict]):
    """uses the files datastructure that's used in the callback to
    list any files that are reported as assets. If the file is not
    reported it's considered not needed, and deleted.
    """
    _fnames_to_keep = list()
    for elt in to_keep:
        assets = elt["assets"]
        for asset in assets:
            fp = Path(asset["path"])
            _fnames_to_keep.append(fp.name)
    for i in assets_dir.glob("*"):
        if i.name not in _fnames_to_keep:
            logger.info(f"Cleaning up {i.as_posix()}")
            os.remove(i)


@task
def print_t(t):
    """dumb function to print stuff..."""
    logger.info("++++++++++++++++++++++++++++++++++++++++")
    logger.info(t)


@task
def generate_callback_body(
    token: str,
    callback_url: str,
    input_dir: str,
    inputs: List[Path],
    jpeg_locs: List[Path],
    small_thumb_locs: List[Path],
):
    """
    TODO, this should be **kwargs, with the type keying.
    Upon completion of file conversion a callback is made to the calling
    API specifying the locations of files, along with metadata about the files.
    the body of the callback should look something like this:
    {
        "status": "success",
        "files":
        [
            {
                "primaryFilePath: "Lab/PI/Myproject/MySession/Sample1/file_a.dm4",
                "title": "file_a",
                "assets":
                [
                    {
                        "type": "keyImage",
                        "path": "Lab/PI/Myproject/MySession/Sample1/file_a.jpg"
                    },
                    {
                        "type": "thumbnail",
                        "path": "Lab/PI/Myproject/MySession/Sample1/file_a_s.jpg"
                    }
                ]
            }
        ]
    }
    """
    files = _gen_callback_file_list(dname=input_dir, inputs=inputs)
    files = _add_outputs(
        dname=input_dir, files=files, outputs=jpeg_locs, _type="keyImage"
    )
    files = _add_outputs(
        dname=input_dir, files=files, outputs=small_thumb_locs, _type="thumbnail"
    )
    data = {"files": files}
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = requests.post(callback_url, headers=headers, data=json.dumps(data))
    logger.info(response.url)
    logger.info(response.status_code)
    logger.info(json.dumps(data))
    logger.info(response.text)
    logger.info(response.headers)
    return files
