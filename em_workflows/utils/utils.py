from em_workflows.file_path import FilePath
from jinja2 import Environment, FileSystemLoader
import subprocess
import requests
import os
import shutil
import json
import prefect
import logging
import datetime
from typing import List, Dict, Set, Optional
from pathlib import Path
from prefect import Flow, task, context
from prefect.triggers import any_successful, always_run
from prefect.engine.state import State, Success
from prefect.engine import signals
from prefect.engine.signals import SKIP, TRIGGERFAIL
from prefect.tasks.control_flow.filter import FilterTask

from em_workflows.config import Config
from collections import namedtuple

# used for keeping outputs of imod's header command (dimensions of image).
Header = namedtuple("Header", "x y z")


filter_results = FilterTask(
    filter_func=lambda x: not isinstance(
        x, (BaseException, TRIGGERFAIL, SKIP, type(None))
    )
)


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
        raise signals.FAIL(msg)
    else:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"Command ok : {stderr} -- {stdout}"
        log(msg)
        xyz_dim = [int(x) for x in stdout.split()]
        xyz_cleaned = Header(*xyz_dim)
        log(f"dims: {xyz_cleaned:}")
        return xyz_cleaned


@task
def mrc_to_movie(file_path: FilePath, root: str, asset_type: str):
    """
    :param file_path: FilePath for the input
    :param root: base name of the mrc file
    :param asset_type: type of resulting output (movie)

    - Uses the file_path to identify the working_dir which should have the "root" mrc
    - Runs IMOD ``mrc2tif`` to convert the mrc to many jpgs named by z number
    - Calls ``ffmpeg`` to create the mp4 movie from the jpgs and returns it as an asset
    """
    mp4 = f"{file_path.working_dir}/{file_path.base}_mp4"
    mrc = f"{file_path.working_dir}/{root}.mrc"
    log_file = f"{file_path.working_dir}/recon_mrc2tiff.log"
    cmd = [Config.mrc2tif_loc, "-j", "-C", "0,255", mrc, mp4]
    FilePath.run(cmd=cmd, log_file=log_file)
    mov = f"{file_path.working_dir}/{file_path.base}_{asset_type}.mp4"
    test_p = Path(f"{file_path.working_dir}/{file_path.base}_mp4.1000.jpg")
    mp4_input = f"{file_path.working_dir}/{file_path.base}_mp4.%03d.jpg"
    if test_p.exists():
        mp4_input = f"{file_path.working_dir}/{file_path.base}_mp4.%04d.jpg"
    cmd = [
        "ffmpeg",
        "-f",
        "image2",
        "-framerate",
        "8",
        "-i",
        mp4_input,
        "-vcodec",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-s",
        "1024,1024",
        mov,
    ]
    log_file = f"{file_path.working_dir}/{file_path.base}_{asset_type}.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(mov))
    asset = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_fp)
    return asset


@task
def gen_prim_fps(fp_in: FilePath) -> Dict:
    """
    :param fp_in: FilePath of current input
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


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=10), trigger=always_run)
def cleanup_workdir(fps: List[FilePath]):
    """
    :param fp: a FilePath which has a working_dir to be removed

    | working_dir isn't needed after run, so rm.
    | task wrapper on the FilePath rm_workdir method.

    """
    if prefect.context.parameters.get("keep_workdir") is True:
        log("keep_workdir is set to True, skipping removal.")
    else:
        for fp in fps:
            log(f"Trying to remove {fp.working_dir}")
            fp.rm_workdir()


def update_adoc(
    adoc_fp: Path,
    tg_fp: Path,
    montage: int,
    gold: int,
    focus: int,
    fiducialless: int,
    trackingMethod: int,
    TwoSurfaces: int,
    TargetNumberOfBeads: int,
    LocalAlignments: int,
    THICKNESS: int,
) -> Path:
    """
    | Uses jinja templating to update the adoc file with input params.
    | dual_p is calculated by inputs_paired() and is used to define `dual`
    | Some of these parameters are derived programatically.

    :todo: Remove references to ``dual_p`` in comments?
    """
    file_loader = FileSystemLoader(str(adoc_fp.parent))
    env = Environment(loader=file_loader)
    template = env.get_template(adoc_fp.name)

    name = tg_fp.stem
    currentBStackExt = None
    stackext = tg_fp.suffix[1:]
    # if dual_p:
    #    dual = 1
    #    currentBStackExt = tg_fp.suffix[1:]  # TODO - assumes both files are same ext
    datasetDirectory = adoc_fp.parent
    if int(TwoSurfaces) == 0:
        SurfacesToAnalyze = 1
    elif int(TwoSurfaces) == 1:
        SurfacesToAnalyze = 2
    else:
        raise signals.FAIL(
            f"Unable to resolve SurfacesToAnalyze, TwoSurfaces \
                is set to {TwoSurfaces}, and should be 0 or 1"
        )
    rpa_thickness = int(int(THICKNESS) * 1.5)

    vals = {
        "name": name,
        "stackext": stackext,
        "currentBStackExt": currentBStackExt,
        "montage": montage,
        "gold": gold,
        "focus": focus,
        "datasetDirectory": datasetDirectory,
        "fiducialless": fiducialless,
        "trackingMethod": trackingMethod,
        "TwoSurfaces": TwoSurfaces,
        "TargetNumberOfBeads": TargetNumberOfBeads,
        "SurfacesToAnalyze": SurfacesToAnalyze,
        "LocalAlignments": LocalAlignments,
        "rpa_thickness": rpa_thickness,
        "THICKNESS": THICKNESS,
    }

    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{tg_fp.stem}.adoc")
    log("Created adoc: adoc_loc.as_posix()")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
    log(f"generated {adoc_loc}")
    return adoc_loc


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
            raise signals.FAIL(f"Files missing. {fp_1},{fp_2}. BRT run failure.")
    return new_loc


def copy_template(working_dir: Path, template_name: str) -> Path:
    """
    :param working_dir: libpath.Path of temporary working directory
    :param template_name: base str name of the ADOC template
    :return: libpath.Path of the copied file

    copies the template adoc file to the working_dir
    """
    adoc_fp = f"{working_dir}/{template_name}.adoc"
    template_fp = f"{Config.template_dir}/{template_name}.adoc"
    log(f"trying to copy {template_fp} to {adoc_fp}")
    shutil.copyfile(template_fp, adoc_fp)
    return Path(adoc_fp)


@task
def run_brt(
    file_path: FilePath,
    adoc_template: str,
    montage: int,
    gold: int,
    focus: int,
    fiducialless: int,
    trackingMethod: int,
    TwoSurfaces: int,
    TargetNumberOfBeads: int,
    LocalAlignments: int,
    THICKNESS: int,
) -> None:
    """
    The natural place for this function is within the brt flow.
    The reason for this is to facilitate testing. In prefect 1, a
    flow lives within a context. This causes problems if things are mocked
    for testing. If the function is in utils, these problems go away.
    TODO, this is ugly. This might vanish in Prefect 2, since flows are
    no longer obligated to being context dependant.
    """

    adoc_fp = copy_template(
        working_dir=file_path.working_dir, template_name=adoc_template
    )
    updated_adoc = update_adoc(
        adoc_fp=adoc_fp,
        tg_fp=file_path.fp_in,
        montage=montage,
        gold=gold,
        focus=focus,
        fiducialless=fiducialless,
        trackingMethod=trackingMethod,
        TwoSurfaces=TwoSurfaces,
        TargetNumberOfBeads=TargetNumberOfBeads,
        LocalAlignments=LocalAlignments,
        THICKNESS=THICKNESS,
    )
    # why do we need to copy these?
    copy_tg_to_working_dir(fname=file_path.fp_in, working_dir=file_path.working_dir)

    # START BRT (Batchruntomo) - long running process.
    cmd = [Config.brt_binary, "-di", updated_adoc.as_posix(), "-cp", "1", "-gpu", "1"]
    log_file = f"{file_path.working_dir}/brt_run.log"
    FilePath.run(cmd, log_file)
    rec_file = Path(f"{file_path.working_dir}/{file_path.base}_rec.mrc")
    ali_file = Path(f"{file_path.working_dir}/{file_path.base}_ali.mrc")
    log(f"checking that dir {file_path.working_dir} contains ok BRT run")

    for _file in [rec_file, ali_file]:
        if not _file.exists():
            raise signals.FAIL(f"File {_file} does not exist. BRT run failure.")
    # brts_ok = check_brt_run_ok(file_path=file_path)


# def check_brt_run_ok(file_path: FilePath):
#     """
#     ensures the following files exist:
#     BASENAME_rec.mrc - the source for the reconstruction movie
#     and Neuroglancer pyramid
#     BASENAME_ali.mrc
#     """
#     rec_file = Path(f"{file_path.working_dir}/{file_path.base}_rec.mrc")
#     ali_file = Path(f"{file_path.working_dir}/{file_path.base}_ali.mrc")
#     log(f"checking that dir {file_path.working_dir} contains ok BRT run")
#
#     for _file in [rec_file, ali_file]:
#         if not _file.exists():
#             raise signals.FAIL(f"File {_file} does not exist. BRT run failure.")


def log(msg):
    """
    Convenience function to print an INFO message to both the "input_dir" context log and
    the "root" prefect log.

    :param msg: string to output
    :return: None
    """
    if hasattr(context, "parameters"):
        logger = logging.getLogger(context.parameters["input_dir"])
        logger.info(msg)
    context.logger.info(msg)


@task(max_retries=1, retry_delay=datetime.timedelta(seconds=10), trigger=always_run)
def copy_workdirs(file_path: FilePath) -> Path:
    """
    :param file_path: The FilePath of the file whose workdir is to be copied
    :returns: Resulting Assets FilePath

    This test copies the workdir, in it's entirety, to the Assets path. This can
    be a very large number of files and storage space. This work is delgated to
    FilePath.

    :param file_path: FilePath of the current imagefile
    :return: pathlib.Path of copied directory

    """
    return file_path.copy_workdir_to_assets()


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
                raise signals.FAIL(
                    f"Expected file: {single_file}, not found in input_dir"
                )
            else:
                _files.append(fp)
    else:
        log(f"Looking for *.{exts} in {input_dir}")
        for ext in exts:
            _files.extend(input_dir.glob(f"*.{ext}"))
    if not _files:
        raise signals.FAIL(f"Input dir {input_dir} not contain anything to process.")
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
        raise signals.FAIL(f"Unable to find any subdirs in dir: {input_dir_fp}")
    log(f"Found {dirs}")
    return dirs


# @task
# def run_single_file(input_fps: List[Path], fp_to_check: str) -> List[Path]:
#     """
#     :param input_fps: List of Paths
#     :param fp_to_check: the filename to check as a str
#     :returns: the input filename in a single-element Path list
#
#     :todo: Consider removing since this function isn't currently called (according to PyCharm)
#
#     Workflows can be run on single files, if the file_name param is used.
#     This function will limit the list of inputs to only that file_name (if
#     provided), and check the file exists, if so will return as Path, else
#     raise exception."""
#     if fp_to_check is None:
#         return input_fps
#     for _fp in input_fps:
#         if _fp.name == fp_to_check:
#             return [Path(fp_to_check)]
#     raise signals.FAIL(f"Expecting file: {fp_to_check}, not found in input_dir")


def notify_api_running(flow: Flow, old_state, new_state) -> State:
    """
    tells API the workflow has started to run.
    """
    if new_state.is_running():
        if prefect.context.parameters.get("no_api"):
            log("no_api flag used, not interacting with API")
        else:
            callback_url = prefect.context.parameters.get("callback_url")
            token = prefect.context.parameters.get("token")
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
                msg = f"Bad response code on notify_api_running: {response}"
                log(msg=msg)
                raise signals.FAIL(msg)
    return new_state


def custom_terminal_state_handler(
    flow: Flow,
    state: State,
    reference_task_states: Set[State],
) -> Optional[State]:
    """
    we define any success at all to be a success
    """
    success = False
    # iterate through reference task states looking for successes
    for task_state in reference_task_states:
        if task_state.is_successful():
            success = True
    if success:
        message = "success"
        ns = Success(
            message=message,
            result=state.result,
            context=state.context,
            cached_inputs=state.cached_inputs,
        )
    else:
        message = "error"
        ns = state
    if prefect.context.parameters.get("no_api"):
        log(f"no_api flag used, terminal: success is {message}")
    else:
        callback_url = prefect.context.parameters.get("callback_url")
        token = prefect.context.parameters.get("token")
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }
        response = requests.post(
            callback_url, headers=headers, data=json.dumps({"status": message})
        )
        log(f"Pipeline status is:{message}, {response.text}")
        log(response.headers)
        if not response.ok:
            msg = f"Bad response code on callback: {response}"
            log(msg=msg)
            raise signals.FAIL(msg)
    return ns


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
        if new_state.is_successful():
            status = "success"
        else:
            status = "error"
        if prefect.context.parameters.get("no_api"):
            log(f"no_api flag used, completion: {status}")
        else:
            callback_url = prefect.context.parameters.get("callback_url")
            token = prefect.context.parameters.get("token")
            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
            }
            response = requests.post(
                callback_url, headers=headers, data=json.dumps({"status": status})
            )
            log(f"Pipeline status is:{status}")
            log(response.text)
            log(response.headers)
            if not response.ok:
                msg = f"Bad response code on callback: {response}"
                log(msg=msg)
                raise signals.FAIL(msg)
    return new_state


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
    return Path(input_path_str)


@task
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


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=1), trigger=any_successful)
def send_callback_body(
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
    if prefect.context.parameters.get("no_api"):
        log("no_api flag used, not interacting with API")
        log(json.dumps(data))
    elif callback_url and token:
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
            raise signals.FAIL(msg)
    else:
        raise signals.FAIL(
            "Invalid state - need callback_url and token, OR set no_api to True."
        )
