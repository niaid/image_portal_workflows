import subprocess
from collections import namedtuple
import requests
import os
import shutil
import json
from typing import List, Dict
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from prefect import task, get_run_logger, allow_failure
from prefect.exceptions import MissingContextError
from prefect.states import State
from prefect.flows import Flow, FlowRun
from prefect.tasks import Task, TaskRun
from prefect.runtime import flow_run

from em_workflows.config import Config
from em_workflows.file_path import FilePath

# used for keeping outputs of imod's header command (dimensions of image).
Header = namedtuple("Header", "x y z")
BrtOutput = namedtuple("BrtOutput", ["ali_file", "rec_file"])


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


@task(
    name="mrc to movie generation",
    on_failure=[collect_exception_task_hook],
)
def mrc_to_movie(file_path: FilePath, root: str, asset_type: str, **kwargs):
    """
    :param file_path: FilePath for the input
    :param root: base name of the mrc file
    :param asset_type: type of resulting output (movie)
    :param kwargs: additional arguments to wait for before executing this func

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
        Config.ffmpeg_loc,
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
def gen_prim_fps(fp_in: FilePath, additional_assets:(dict, ...)=None) -> Dict:
    """
    :param fp_in: FilePath of current input
    :param additional_assets: A list of additional assets to be added to the primary element
    :outputs_with_exceptions this func is called with allow_failure - important
    :return: a Dict to hold the 'assets'

    This function delegates creation of primary fps to ``FilePath.gen_prim_fp_elt()``
    and creates a primary element for assets to be appended
    """
    base_elts = fp_in.gen_prim_fp_elt()
    log(f"Generated fp elt {base_elts}")

    for asset in additional_assets:
        add_asset.fn(base_elts, asset)

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


# triggers like "always_run" are managed when calling the task itself
@task(retries=3, retry_delay_seconds=10)
def cleanup_workdir(fps: List[FilePath], x_keep_workdir: bool):
    """
    :param fp: a FilePath which has a working_dir to be removed

    | working_dir isn't needed after run, so remove unless "x_keep_workdir" is True.
    | task wrapper on the FilePath rm_workdir method.

    """
    if x_keep_workdir is True:
        log("x_keep_workdir is set to True, skipping removal.")
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
        raise ValueError(
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
            raise RuntimeError(f"Files missing. {fp_1},{fp_2}. BRT run failure.")
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


@task(
    name="Batchruntomo conversion",
    tags=["brt"],
    # timeout_seconds=600,
    on_failure=[collect_exception_task_hook],
)
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
) -> BrtOutput:
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
    cmd = [Config.brt_binary, "-di", updated_adoc.as_posix(), "-cp", "60", "-gpu", "1"]
    log_file = f"{file_path.working_dir}/brt_run.log"
    FilePath.run(cmd, log_file)
    rec_file = Path(f"{file_path.working_dir}/{file_path.base}_rec.mrc")
    ali_file = Path(f"{file_path.working_dir}/{file_path.base}_ali.mrc")
    log(f"checking that dir {file_path.working_dir} contains ok BRT run")

    for _file in [rec_file, ali_file]:
        if not _file.exists():
            raise ValueError(f"File {_file} does not exist. BRT run failure.")
    return BrtOutput(ali_file=ali_file, rec_file=rec_file)


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
    log(f"found {len(_files)} files")
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


# def custom_terminal_state_handler(
# ) -> Optional[State]:
#     """
#     we define any success at all to be a success
#     """
#     success = False
#     # iterate through reference task states looking for successes
#     for task_state in reference_task_states:
#         if task_state.is_successful():
#             success = True
#     if success:
#         message = "success"
#         ns = Success(
#             message=message,
#             result=state.result,
#             context=state.context,
#             cached_inputs=state.cached_inputs,
#         )
#     else:
#         message = "error"
#         ns = state
#     if prefect.context.parameters.get("x_no_api"):
#         log(f"x_no_api flag used, terminal: success is {message}")
#     else:
#         callback_url = prefect.context.parameters.get("callback_url")
#         token = prefect.context.parameters.get("token")
#         headers = {
#             "Authorization": "Bearer " + token,
#             "Content-Type": "application/json",
#         }
#         response = requests.post(
#             callback_url, headers=headers, data=json.dumps({"status": message})
#         )
#         log(f"Pipeline status is:{message}, {response.text}")
#         log(response.headers)
#         if not response.ok:
#             msg = f"Bad response code on callback: {response}"
#             log(msg=msg)
#             raise signals.FAIL(msg)
#     return ns


def notify_api_completion(flow: Flow, flow_run: FlowRun, state: State):
    """
    https://docs.prefect.io/core/concepts/states.html#overview.
    https://docs.prefect.io/core/concepts/notifications.html#state-handlers
    """
    status = "success" if state.is_completed() else "error"
    x_no_api = flow_run.parameters.get("x_no_api", False)
    token = flow_run.parameters.get("token", "")
    callback_url = flow_run.parameters.get("callback_url", "")

    flowrun_id = os.environ.get("PREFECT__FLOW_RUN_ID", "not-found")

    if x_no_api:
        log(f"x_no_api flag used\nCompletion status: {status}")
        return

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


# TODO handle "trigger=any_successful"
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
    stored_result = Config.local_storage.read_path(f"{flow_run.id}__gen_fps")
    fps: List[FilePath] = Config.pickle_serializer.loads(
        json.loads(stored_result)["data"].encode()
    )
    parameters = flow_run.parameters
    x_keep_workdir = parameters.get("x_keep_workdir", False)

    for fp in fps:
        copy_workdir_logs.fn(file_path=fp)

    cleanup_workdir.fn(fps, x_keep_workdir)


def callback_with_cleanup(
    fps: List[FilePath],
    callback_result: List,
    x_no_api: bool = False,
    callback_url: str = None,
    token: str = None,
    x_keep_workdir: bool = False,
):
    cp_wd_logs_to_assets = copy_workdir_logs.map(fps, wait_for=[callback_result])

    cb = send_callback_body.submit(
        x_no_api=x_no_api,
        token=token,
        callback_url=callback_url,
        files_elts=callback_result,
    )
    cleanup_workdir.submit(
        fps,
        x_keep_workdir,
        wait_for=[cb, allow_failure(cp_wd_logs_to_assets)],
    )


def generate_flow_run_name():
    parameters = flow_run.parameters
    name = Path(parameters["input_dir"])
    share_name = parameters["file_share"]
    return f"{share_name} | {name.parts[-2]} / {name.parts[-1]}"
