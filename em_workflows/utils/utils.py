import subprocess
from collections import namedtuple
import requests
import os
import shutil
import json
from typing import List, Dict
from pathlib import Path

from prefect import task, get_run_logger
from prefect.exceptions import MissingContextError
from prefect.states import State
from prefect.flows import Flow, FlowRun
from prefect.tasks import Task, TaskRun
from prefect.runtime import flow_run

from em_workflows.config import Config
from em_workflows.file_path import FilePath

# used for keeping outputs of imod's header command (dimensions of image).
Header = namedtuple("Header", "x y z")


def log(msg):
    """
    Convenience function to print an INFO message to both the "input_dir" context log and
    the "root" prefect log.

    :param msg: string to output
    :return: None
    """
    try:
        get_run_logger().info(msg)
    except MissingContextError:
        print(msg)


def lookup_dims(fp: Path) -> Header:
    """
    :param fp: pathlib.Path to an image
    :returns: a tuple containing x,y,z dims of file

    calls IMOD ``header`` with -s (size) flag; parses stdout to get result
    """
    cmd = [Config.header_loc, "-s", fp]
    sp = subprocess.run(cmd, check=False, capture_output=True)
    if sp.returncode != 0:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"ERROR : {stderr} -- {stdout}"
        log(msg)
        raise RuntimeError(msg)
    else:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"Command ok : {stderr} -- {stdout}"
        log(msg)
        xyz_dim = [int(x) for x in stdout.split()]
        xyz_cleaned = Header(*xyz_dim)
        log(f"dims: {xyz_cleaned:}")
        return xyz_cleaned


def collect_exception_task_hook(task: Task, task_run: TaskRun, state: State):
    """
    This task hook should be used with tasks where you intend to know which step of the flow run broke.
    Since most of our tasks are mapped by default using filepaths, it takes map index into account as well
    So that we can notify the user, 'this step of this file broke'.
    The message is written to a file using prefect's local storage.
    In order to retrieve it, the flow needs to have a logic at the end,
        to lookup for this file with exception message if the task run has failed.
    """
    message = f"Failure in pipeline step: {task.name}"
    map_idx = task_run.name.split("-")[-1]
    flow_run_id = state.state_details.flow_run_id
    path = f"{flow_run_id}__{map_idx}"
    try:
        Config.local_storage.read_path(path)
    except ValueError:  # ValueError path not found
        Config.local_storage.write_path(path, message.encode())


@task
def gen_prim_fps(fp_in: FilePath) -> Dict:
    """
    :param fp_in: FilePath of current input
    :outputs_with_exceptions this func is called with allow_failure - important
    :return: a Dict to hold the 'assets'

    This function delegates creation of primary fps to ``FilePath.gen_prim_fp_elt()``
    and creates a primary element for assets to be appended
    """
    base_elts = fp_in.gen_prim_fp_elt()
    log(f"Generated fp elt {base_elts}")
    return base_elts


@task
def add_imageSet(prim_fp: dict, imageSet: list) -> Dict:
    """
    :param prim_fp: the 'primary' element, describing input file location
    :param imageSet: list of scenes
    :return: prim_fp with asset added
    """
    prim_fp["imageSet"] = imageSet
    return prim_fp


@task
def add_asset(prim_fp: dict, asset: dict, image_idx: int = None) -> dict:
    """
    :param prim_fp: the 'primary' element (dict) to which assets are appended
    :param asset: The actual asset (output) to be added in the form of another dict
    :return: The resulting, dict with asset added

    This function is used to add elements to the callback datastructure, for the Hewwig API:

    - This datastructure is a dict, and is converted to JSON just prior to sending to API.
    - Asset types are checked to ensure they are valid, else we complain.
    - Metadata is required (only) for the neuroglancer type asset.
    - The function generates a dict called "asset", which has keys: path, type, and maybe metdata.
    - The value of type is used from method signature (above).
    - The value of path is the location of the asset, relative to the "Assets" dir on the RML filesystem.
    - Every asset element is added to the key "assets"
    - Asset `type` can be one of:

        - averagedVolume
        - keyImage
        - keyThumbnail
        - recMovie
        - tiltMovie
        - volume
        - neuroglancerPrecomputed

    **Note**: when running Dask distributed with Slurm, mutations to objects will be lost. Using
    funtional style avoids this. This is why the callback data structure is not built inside
    the FilePath object at runtime.
    """

    if not image_idx:
        image_idx = 0
    if type(asset) is list:
        prim_fp["imageSet"][image_idx]["assets"].extend(asset)
    else:
        prim_fp["imageSet"][image_idx]["assets"].append(asset)
    log(f"Added fp elt {asset} to {prim_fp}, at index {image_idx}.")
    return prim_fp


def cleanup_workdir(fp: FilePath, x_keep_workdir: bool):
    """
    :param fp: a FilePath which has a working_dir to be removed

    | working_dir isn't needed after run, so remove unless "x_keep_workdir" is True.
    | task wrapper on the FilePath rm_workdir method.

    """
    if x_keep_workdir is True:
        log("x_keep_workdir is set to True, skipping removal.")
    else:
        log(f"Trying to remove {fp.working_dir}")
        fp.rm_workdir()


def copy_tg_to_working_dir(fname: Path, working_dir: Path) -> Path:
    """
    copies files (tomograms/mrc files) into working_dir
    returns Path of copied file
    :todo: Determine if the 'a' & 'b' files still exist and if these files need
    to be copied. (See comment in ``run_brt`` before this call is made)
    """
    new_loc = Path(f"{working_dir}/{fname.name}")
    if fname.exists():
        shutil.copyfile(src=fname.as_posix(), dst=new_loc)
    else:
        fp_1 = Path(f"{fname.parent}/{fname.stem}a{fname.suffix}")
        fp_2 = Path(f"{fname.parent}/{fname.stem}b{fname.suffix}")
        if fp_1.exists() and fp_2.exists():
            shutil.copyfile(src=fp_1.as_posix(), dst=f"{working_dir}/{fp_1.name}")
            shutil.copyfile(src=fp_2.as_posix(), dst=f"{working_dir}/{fp_2.name}")
        else:
            raise RuntimeError(f"Files missing. {fp_1},{fp_2}. BRT run failure.")
    return new_loc


# TODO replace "trigger=always_run"
@task(retries=1, retry_delay_seconds=10)
def copy_workdirs(file_path: FilePath) -> Path:
    """
    This task copies the workdir, in it's entirety, to the Assets path. This can
    be a very large number of files and storage space. This work is delgated to
    FilePath.

    Primarily, used by brt-flow where SME had to deal with intermediate files
    for sanity checks.

    :param file_path: FilePath of the current imagefile
    :return: pathlib.Path of copied directory
    """
    return file_path.copy_workdir_to_assets()


@task
def copy_workdir_logs(file_path: FilePath) -> Path:
    """
    :param file_path: FilePath of the current imagefile
    :return: pathlib.Path of copied directory

    This task copies the logs of intermediate commands ran during the workflow.
    Skips rest of the files unlike `file_path.copy_workdirs`
    """
    return file_path.copy_workdir_logs_to_assets()


@task
def list_files(input_dir: Path, exts: List[str], single_file: str = None) -> List[Path]:
    """
    :param input_dir: libpath.Path of the input directory
    :param exts: List of str extensions to be checked
    :param single_file: if present, only that file returned
    :return: List of pathlib.Paths of matching files

    - List all files within input_dir with specified extension.
    - if a specific file is requested that file is returned only.
    - This allows workflows to run on single files rather than entire dirs (default).
    - Note, if no files are found does NOT raise exception. Function can be called
      multiple times, sometimes there will be no files of that extension.
    """
    _files = list()
    if single_file:
        log(f"Looking for single file: {single_file} in {input_dir}")
        fp = Path(f"{input_dir}/{single_file}")
        ext = fp.suffix.strip(".")
        if ext in exts:
            if not fp.exists():
                raise RuntimeError(
                    f"Expected file: {single_file}, not found in input_dir"
                )
            else:
                _files.append(fp)
    else:
        log(f"Looking for *.{exts} in {input_dir}")
        for ext in exts:
            _files.extend(input_dir.glob(f"*.{ext}"))
    if not _files:
        raise RuntimeError(f"Input dir {input_dir} not contain anything to process.")
    log("found files")
    log(_files)
    return _files


@task
def list_dirs(input_dir_fp: Path) -> List[Path]:
    """
    Lists subdirs of directory input_dir
    Some pipelines, eg SEM, store image stacks in dirs (rather than
    single files.
    """
    log(f"trying to list {input_dir_fp}")
    dirs = [Path(x) for x in input_dir_fp.iterdir() if x.is_dir()]
    if len(dirs) == 0:
        raise RuntimeError(f"Unable to find any subdirs in dir: {input_dir_fp}")
    log(f"Found {dirs}")
    return dirs


@task(retries=1, retry_delay_seconds=10)
def notify_api_running(
    x_no_api: bool = None, token: str = None, callback_url: str = None
):
    """
    tells API the workflow has started to run.
    """
    if x_no_api:
        log("x_no_api flag used, not interacting with API")
        return
    elif not callback_url or not token:
        raise RuntimeError(
            "notify_api_running: Either callback_url or token is missing"
        )
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
    }
    response = requests.post(
        callback_url, headers=headers, data=json.dumps({"status": "running"})
    )
    log(response.text)
    log(response.headers)
    if not response.ok:
        msg = f"Bad response on notify_api_running: {response}"
        log(msg=msg)
        raise RuntimeError(msg)
    return response.ok


def notify_api_completion(flow: Flow, flow_run: FlowRun, state: State) -> bool:
    """
    When the state changes for a workflow, this hook calls the backend api to update status
    of the workflow in its db.

    The params for state change hooks can be found here:
    https://docs.prefect.io/latest/guides/state-change-hooks/
    """
    status = "success" if state.is_completed() else "error"
    x_no_api = flow_run.parameters.get("x_no_api", False)
    token = flow_run.parameters.get("token", "")
    callback_url = flow_run.parameters.get("callback_url", "")

    flowrun_id = os.environ.get("PREFECT__FLOW_RUN_ID", "not-found")

    if x_no_api:
        log(f"x_no_api flag used\nCompletion status: {status}")
        return True

    hooks_log = open(f"slurm-log/{flowrun_id}-notify-api-completion.txt", "w")
    hooks_log.write(f"Trying to notify: {x_no_api=}, {token=}, {callback_url=}\n")

    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
    }
    response = requests.post(
        callback_url, headers=headers, data=json.dumps({"status": status})
    )
    hooks_log.write(f"Pipeline status is:{status}\n")
    hooks_log.write(f"{response.ok=}\n")
    hooks_log.write(f"{response.status_code=}\n")
    hooks_log.write(f"{response.text=}\n")
    hooks_log.write(f"{response.headers=}\n")

    if not response.ok:
        msg = f"Bad response code on callback: {response}"
        log(msg=msg)
        hooks_log.write(f"{msg}\n")
        hooks_log.close()
        raise RuntimeError(msg)

    hooks_log.close()
    return response.ok


def get_environment() -> str:
    """
    The workflows can operate in one of several environments,
    named HEDWIG_ENV for historical reasons, eg prod, qa or dev.
    This function looks up that environment.
    Raises exception if no environment found.
    """
    env = os.environ.get("HEDWIG_ENV")
    if not env:
        raise RuntimeError(
            "Unable to look up HEDWIG_ENV. Should \
                be exported set to one of: [dev, qa, prod]"
        )
    return env


@task
def get_input_dir(share_name: str, input_dir: str) -> Path:
    """
    :param share_name:
    :param input_dir:
    :return: Path to complete project directory (obtained from share-name)
        combined with relative input directory

    | Concat the POSTed input file path to the mount point.
    | create working dir
    | returns Path obj
    """
    if not input_dir.endswith("/"):
        input_dir = input_dir + "/"
    if not input_dir.startswith("/"):
        input_dir = "/" + input_dir
    input_path_str = Config.proj_dir(share_name=share_name) + input_dir
    p = Path(input_path_str)
    log(f"Checking input dir {p.as_posix()} which exists? {p.exists()}")
    if not p.exists():
        raise RuntimeError(f"Fail get_input_dir, {p.as_posix()} does not exist")
    return p


@task(
    # persisting to retrieve again in hooks
    persist_result=True,
    result_storage=Config.local_storage,
    result_serializer=Config.pickle_serializer,
    result_storage_key="{flow_run.id}__gen_fps",
)
def gen_fps(share_name: str, input_dir: Path, fps_in: List[Path]) -> List[FilePath]:
    """
    Given in input directory (Path) and a list of input files (Path), return
    a list of FilePaths for the input files. This includes a temporary working
    directory for each file to keep the files separate on the HPC.
    """
    fps = list()
    for fp in fps_in:
        file_path = FilePath(share_name=share_name, input_dir=input_dir, fp_in=fp)
        msg = f"created working_dir {file_path.working_dir} for {fp.as_posix()}"
        log(msg)
        fps.append(file_path)
    return fps


@task(retries=3, retry_delay_seconds=60)
def send_callback_body(
    x_no_api: bool,
    files_elts: List[Dict],
    token: str = None,
    callback_url: str = None,
) -> None:
    """
    Upon completion of file conversion a callback is made to the calling
    API specifying the locations of files, along with metadata about the files.
    the body of the callback should look something like this:

    .. code-block::
        Refer to docs/demo_callback.json for expected
    """
    data = {"files": files_elts}
    if x_no_api is True:
        log("x_no_api flag used, not interacting with API")
        log(json.dumps(data))
        return

    if callback_url and token:
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }
        response = requests.post(callback_url, headers=headers, data=json.dumps(data))
        log(response.url)
        log(response.status_code)
        log(json.dumps(data))
        log(response.text)
        log(response.headers)
        if not response.ok:
            msg = f"Bad response code on callback: {response}"
            log(msg=msg)
            raise RuntimeError(msg)
    else:
        raise RuntimeError(
            "Invalid state - need callback_url and token, OR set x_no_api to True."
        )


def copy_workdirs_and_cleanup_hook(flow, flow_run, state):
    """
    A flow state change hook called to copy workdir log files and also cleanup workdir after coppying.
    """
    stored_result = Config.local_storage.read_path(f"{flow_run.id}__gen_fps")
    fps: List[FilePath] = Config.pickle_serializer.loads(
        json.loads(stored_result)["data"].encode()
    )
    parameters = flow_run.parameters
    x_keep_workdir = parameters.get("x_keep_workdir", False)

    for fp in fps:
        copy_workdir_logs.fn(file_path=fp)
        cleanup_workdir(fp, x_keep_workdir)


def generate_flow_run_name():
    """
    Custom flow run names generator to replace default behavior, which is it simply uses flow function name

    https://docs.prefect.io/latest/concepts/flows/#flow-settings
    """
    parameters = flow_run.parameters
    name = Path(parameters["input_dir"])
    share_name = parameters["file_share"]
    return f"{share_name} | {name.parts[-2]} / {name.parts[-1]}"
