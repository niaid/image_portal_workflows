from em_workflows.file_path import FilePath
from jinja2 import Environment, FileSystemLoader
import subprocess
import re
import requests
import os
import shutil
import json
import prefect
import logging
import datetime
from typing import List, Dict, Set, Optional
from pathlib import Path
from prefect import task, context
from prefect import Flow, task, context
from prefect.triggers import any_successful, always_run, any_failed
from prefect.engine.state import State, Success
from prefect.engine import signals
from prefect.engine.signals import SKIP, TRIGGERFAIL
from em_workflows.config import Config
from prefect.tasks.control_flow.filter import FilterTask

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
    returns tuple containing x,y,z dims of file
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
        xyz_dim = re.split(" +(\d+)", stdout)
        xyz_cleaned = Header(int(xyz_dim[1]), int(xyz_dim[3]), int(xyz_dim[5]))
        log(f"dims: {xyz_cleaned:}")
        return xyz_cleaned


@task
def mrc_to_movie(file_path: FilePath, root: str, asset_type: str):
    """
    converts an mrc file into a movie
    root should be mrc_fname
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
    return fp_in.gen_prim_fp_elt()


@task
def add_asset(prim_fp: dict, asset: dict) -> dict:
    prim_fp["assets"].append(asset)
    return prim_fp


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=10), trigger=always_run)
def cleanup_workdir(fp: FilePath):
    """
    working_dir isn't needed after run, so rm.
    task wrapper on the FilePath rm_workdir method.
    """
    log(f"Trying to remove {fp.working_dir}")
    fp.rm_workdir()


# @task
# def check_inputs_ok(fps: List[Path]) -> None:
#    """
#    ensures there's at least one file that is going to be processed.
#    escapes bad chars that occur in input file names
#    """
#    if not fps:
#        raise signals.FAIL(f"Input dir does not contain anything to process.")
#    for fp in fps:
#        if not fp.exists():
#            raise signals.FAIL(f"Input dir does not contain {fp}")
#    log("files ok")


# @task
# def sanitize_file_names(fps: List[Path]) -> List[Path]:
#    escaped_files = [Path(_escape_str(_file.as_posix())) for _file in fps]
#    return escaped_files


# def _make_work_dir(fname: Path = None) -> Path:
#     """
#     a temporary dir to house all files in the form:
#     {Config.tmp_dir}{fname.stem}.
#     eg: /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/tomogram_fname/
#     Will be rm'd upon completion.
#     """
#     working_dir = Path(tempfile.mkdtemp(dir=f"{Config.tmp_dir}"))
#     if fname:
#         msg = f"created working_dir {working_dir} for {fname.as_posix()}"
#     else:
#         msg = f"No file name given for dir creation"
#     return Path(working_dir)


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
    Uses jinja templating to update the adoc file with input params.
    dual_p is calculated by inputs_paired() and is used to define `dual`
    Some of these parameters are derived programatically.
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
    log(f"Created adoc: adoc_loc.as_posix()")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
    log(f"generated {adoc_loc}")
    return adoc_loc


def copy_tg_to_working_dir(fname: Path, working_dir: Path) -> Path:
    """
    copies files (tomograms/mrc files) into working_dir
    returns Path of copied file
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
    copies the template adoc file to the working_dir,
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
    tomogram_fp = copy_tg_to_working_dir(
        fname=file_path.fp_in, working_dir=file_path.working_dir
    )

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


# @task
# def create_brt_command(adoc_fp: Path) -> str:
#    cmd = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1 &> {adoc_fp.parent}/brt.log"
#    log(f"Generated command: {cmd}")
#    return cmd


# @task
# def add_assets(assets_list: Dict, new_asset: Dict[str, str]) -> Dict:
#     log(f"Trying to add asset {new_asset}")
#     assets_list.get("assets").append(new_asset)
#     return assets_list


# @task
# def gen_callback_elt(input_fname: Path, input_fname_b: Path = None) -> Dict:
#     """
#     creates a single primaryFilePath element, to which assets can be appended.
#     TODO:
#     input_fname_b is optional, sometimes the input can be a pair of files.
#     eg:
#     [
#      {
#       "primaryFilePath": "Lab/PI/Myproject/MySession/Sample1/file_a.mrc",
#       "title": "file_a",
#       "assets": []
#      }
#     ]
#     """
#     title = input_fname.stem  # working for now.
#     proj_dir = Config.proj_dir(env=get_environment())
#     primaryFilePath = input_fname.relative_to(proj_dir)
#     return dict(primaryFilePath=primaryFilePath.as_posix(), title=title, assets=list())


# def _esc_char(match):
#     return "\\" + match.group(0)


def _tr_str(name):
    _to_esc = re.compile(r"\s|[]()[]")
    return _to_esc.sub("_", name)


# def _escape_str(name):
#    _to_esc = re.compile(r"\s|[]()[]")
#    return _to_esc.sub(_esc_char, name)


@task
def init_log(file_path: FilePath) -> None:
    fp = f"{file_path.working_dir.as_posix()}/log.txt"
    log_fp = Path(fp)
    # we are going to clobber previous logs - rm if want to keep
    # if log_fp.exists():
    #    log_fp.unlink()
    # the getLogger function uses the (fairly) unique input_dir to look up.
    logger = logging.getLogger(context.parameters["input_dir"])
    logger.setLevel("INFO")

    if not logger.handlers:
        handler = logging.FileHandler(log_fp, encoding="utf-8")
        logger.addHandler(handler)

        # Formatter can be whatever you want
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d_%H:%M:%S",
        )
        handler.setFormatter(formatter)


# def _init_log(working_dir: Path) -> None:
#     log_fp = Path(working_dir, Path("log.txt"))
#     # we are going to clobber previous logs - rm if want to keep
#     # if log_fp.exists():
#     #    log_fp.unlink()
#     # the getLogger function uses the (fairly) unique input_dir to look up.
#     logger = logging.getLogger(context.parameters["input_dir"])
#     logger.setLevel("INFO")
#
#     if not logger.handlers:
#         handler = logging.FileHandler(log_fp, encoding="utf-8")
#         logger.addHandler(handler)
#
#         # Formatter can be whatever you want
#         formatter = logging.Formatter(
#             "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#             datefmt="%Y-%m-%d_%H:%M:%S",
#         )
#         handler.setFormatter(formatter)


def abbreviate_list(l: List[str]) -> str:
    """
    Abbreviates a long list displaying only first and last elt,
    if for example you want give an idea of what commands are running.
    go from:
    [This, is, a, long, list, that, goes, on, and, on] ->
    to:
    This
    ..
    on
    """
    # this generates huge numbers of commands -
    abbrv = "\n" + l[0] + "\n..\n" + l[-1]
    return abbrv


def log(msg):
    # log_name is defined by the dir_name (all wfs are associated with an input_dir
    logger = logging.getLogger(context.parameters["input_dir"])
    logger.info(msg)
    context.logger.info(msg)


@task(max_retries=1, retry_delay=datetime.timedelta(seconds=10), trigger=always_run)
def copy_workdirs(file_path: FilePath) -> Path:
    return file_path.copy_workdir_to_assets()


# @task(max_retries=1, retry_delay=datetime.timedelta(seconds=10), trigger=always_run)
# def copy_workdir_on_fail(working_dir: Path, assets_dir: Path) -> None:
#    """copies entire contents of working dir to outputs dir"""
#    workd_name = datetime.datetime.now().strftime("work_dir_%I_%M%p_%B_%d_%Y")
#    dest = f"{assets_dir.as_posix()}/{workd_name}"
#    log(f"An error occured - will copy {working_dir} to {dest}")
#    shutil.copytree(working_dir.as_posix(), dest)


@task
def cp_logs_to_assets(working_dir: Path, assets_dir: Path) -> None:
    print(f"looking in {working_dir}")
    print(f"copying to {assets_dir}")
    for _log in working_dir.glob("*.log"):
        print(f"found {_log}")
        print(f"going to copy to {assets_dir}")
        shutil.copy(_log, assets_dir)


@task
def list_files(input_dir: Path, exts: List[str], single_file: str = None) -> List[Path]:
    """
    List all files within input_dir with spefified extension.
    if a specific file is requested that file is returned only.
    This allows workflows run on single files rather than entire dirs (default).
    Note, if no files are found does NOT raise exception. Function can be called
    multiple times, sometimes there will be no files of that extension.
    """
    _files = list()
    log(f"Looking for *.{exts} in {input_dir}")
    if single_file:
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
        for ext in exts:
            _files.extend(input_dir.glob(f"*.{ext}"))
    if not _files:
        raise signals.FAIL(f"Input dir does not contain anything to process.")
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


@task
def gen_output_fp(input_fp: Path, output_ext: str, working_dir: Path = None) -> Path:
    """
    cat working_dir to input_fp.name, but swap the extension to output_ext
    the reason for having a working_dir default to None is sometimes the output
    dir is not the same as the input dir, and working_dir is used to define output
    in this case.
    """
    stem_name = _tr_str(input_fp.stem)
    if working_dir:
        output_fp = f"{working_dir.as_posix()}/{stem_name}{output_ext}"
    else:
        output_fp = f"{input_fp.parent}/{stem_name}{output_ext}"

    msg = f"Using dir: {working_dir}, file: {input_fp}, ext: {output_ext} creating output_fp {output_fp}"
    log(msg=msg)
    return Path(output_fp)


@task
def gen_output_fname(input_fp: Path, output_ext) -> Path:
    """
    Each file is generated using the input file name, without extension,
    with a new extension."""
    output_fp = Path(f"{input_fp.stem}{output_ext}")
    log(f"input: {input_fp} output: {output_fp}")
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
def get_input_dir(input_dir: str) -> Path:
    """
    Concat the POSTed input file path to the mount point.
    create working dir
    set up logger
    returns Path obj
    """
    if not input_dir.endswith("/"):
        input_dir = input_dir + "/"
    if not input_dir.startswith("/"):
        input_dir = "/" + input_dir
    input_path_str = Config.proj_dir(env=get_environment()) + input_dir
    return Path(input_path_str)


@task
def gen_fps(input_dir: Path, fps_in: List[Path]) -> List[FilePath]:
    fps = list()
    for fp in fps_in:
        file_path = FilePath(input_dir=input_dir, fp_in=fp)
        msg = f"created working_dir {file_path.working_dir} for {fp.as_posix()}"
        log(msg)
        fps.append(file_path)
    return fps


# @task
# def set_up_work_env(input_fp: Path) -> Path:
#     """note input"""
#     # create a temp space to work
#     working_dir = _make_work_dir()
#     _init_log(working_dir=working_dir)
#     log(f"Working dir for {input_fp} is {working_dir}.")
#     return working_dir


# @task
# def print_t(t):
#     """dumb function to print stuff..."""
#     log("++++++++++++++++++++++++++++++++++++++++")
#     log(t)


@task
def add_assets_entry(
    base_elt: Dict, path: Path, asset_type: str, metadata: Dict[str, str] = None
) -> Dict:
    """
    asset type can be one of:

    averagedVolume
    keyImage
    keyThumbnail
    recMovie
    tiltMovie
    volume
    neuroglancerPrecomputed

    used to build the callback for API
    metadata is used in conjunction with neuroglancer only
    Used in FilePath obj
    """
    valid_typs = [
        "averagedVolume",
        "keyImage",
        "thumbnail",
        "keyThumbnail",
        "recMovie",
        "tiltMovie",
        "volume",
        "neuroglancerPrecomputed",
    ]
    if asset_type not in valid_typs:
        raise signals.FAIL(
            f"Asset type: {asset_type} is not a valid type. {valid_typs}"
        )
    fp_no_mount_point = path.relative_to(Config.assets_dir(env=get_environment()))
    if metadata:
        asset = {
            "type": asset_type,
            "path": fp_no_mount_point.as_posix(),
            "metadata": metadata,
        }
    else:
        asset = {"type": asset_type, "path": fp_no_mount_point.as_posix()}
    base_elt["assets"].append(asset)
    return base_elt


@task
def make_assets_dir(input_dir: Path, subdir_name: Path = None) -> Path:
    """
    input_dir comes in the form {mount_point}/RMLEMHedwigQA/Projects/Lab/PI/
    want to create: {mount_point}/RMLEMHedwigQA/Assets/Lab/PI/
    Sometimes you don't want to create a subdir based on a file name. (eg fibsem)
    Used in FilePath obj
    """
    if not "Projects" in input_dir.as_posix():
        raise signals.FAIL(
            f"Input directory {input_dir} does not look correct, it must contain the string 'Projects'."
        )
    assets_dir_as_str = input_dir.as_posix().replace("/Projects/", "/Assets/")
    if subdir_name:
        assets_dir = Path(f"{assets_dir_as_str}/{subdir_name.stem}")
    else:
        assets_dir = Path(assets_dir_as_str)
    log(f"making assets dir for {input_dir} at {assets_dir.as_posix()}")
    assets_dir.mkdir(parents=True, exist_ok=True)
    return assets_dir


@task
def copy_to_assets_dir(fp: Path, assets_dir: Path, prim_fp: Path = None) -> Path:
    """
    Copy fp to the assets (reported output) dir
    fp is the Path to be copied.
    assets_dir is the root dir (the input_dir with s/Projects/Assets/)
    If prim_fp is passed, assets will be copied to a subdir defined by the input
    file name, eg:
    copy /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/keyMov_SARsCoV2_1.mp4
    to
    /mnt/ai-fas12/RMLEMHedwigQA/Assets/Lab/Pi/SARsCoV2_1/keyMov_SARsCoV2_1.mp4
    {mount_point}/{dname}/keyMov_SARsCoV2_1.mp4
    (note "SARsCoV2_1" in assets_dir)
    If prim_fp is not used, no such subdir is created.
    """
    if prim_fp is not None:
        assets_sub_dir = Path(f"{assets_dir}/{prim_fp.stem}")
    else:
        assets_sub_dir = assets_dir
    assets_sub_dir.mkdir(exist_ok=True)
    dest = Path(f"{assets_sub_dir}/{fp.name}")
    log(f"Trying to copy {fp} to {dest}")
    if fp.is_dir():
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(fp, dest)
    else:
        shutil.copyfile(fp, dest)
    return dest


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
    data = {"files": files_elts}
    if prefect.context.parameters.get("no_api"):
        log("no_api flag used, not interacting with API")
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
            f"Invalid state - need callback_url and token, OR set no_api to True."
        )
