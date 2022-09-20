import tempfile
import re
import re
import requests
import os
import shutil
import json
import prefect
import logging
import datetime
from typing import List, Dict
from pathlib import Path
from prefect import task, context
from prefect import Flow, task, context
from prefect.triggers import all_successful, always_run, any_failed
from prefect.engine.state import State
from prefect.engine import signals

from em_workflows.config import Config


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=10), trigger=all_successful)
def cleanup_workdir(wd: Path):
    """
    working_dir isn't needed after run, so rm.
    """
    # log(f"Trying to remove {wd}")
    shutil.rmtree(wd)


@task
def check_inputs_ok(fps: List[Path]) -> None:
    """
    ensures there's at least one file that is going to be processed.
    escapes bad chars that occur in input file names
    """
    if not fps:
        raise signals.FAIL(f"Input dir does not contain anything to process.")
    for fp in fps:
        if not fp.exists():
            raise signals.FAIL(f"Input dir does not contain {fp}")
    log("files ok")


@task
def sanitize_file_names(fps: List[Path]) -> List[Path]:
    escaped_files = [Path(_escape_str(_file.as_posix())) for _file in fps]
    return escaped_files


def _make_work_dir(fname: Path = None) -> Path:
    """
    a temporary dir to house all files in the form:
    {Config.tmp_dir}{fname.stem}.
    eg: /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/tomogram_fname/
    Will be rm'd upon completion.
    """
    working_dir = Path(tempfile.mkdtemp(dir=f"{Config.tmp_dir}"))
    if fname:
        msg = f"created working_dir {working_dir} for {fname.as_posix()}"
    else:
        msg = f"No file name given for dir creation"
    return Path(working_dir)


@task
def create_brt_command(adoc_fp: Path) -> str:
    cmd = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1 &> {adoc_fp.parent}/brt.log"
    log(f"Generated command: {cmd}")
    return cmd


@task
def add_assets(assets_list: Dict, new_asset: Dict[str, str]) -> Dict:
    log(f"Trying to add asset {new_asset}")
    assets_list.get("assets").append(new_asset)
    return assets_list


@task
def gen_callback_elt(input_fname: Path, input_fname_b: Path = None) -> Dict:
    """
    creates a single primaryFilePath element, to which assets can be appended.
    TODO:
    input_fname_b is optional, sometimes the input can be a pair of files.
    eg:
    [
     {
      "primaryFilePath": "Lab/PI/Myproject/MySession/Sample1/file_a.mrc",
      "title": "file_a",
      "assets": []
     }
    ]
    """
    title = input_fname.stem  # working for now.
    proj_dir = Config.proj_dir(env=get_environment())
    primaryFilePath = input_fname.relative_to(proj_dir)
    return dict(primaryFilePath=primaryFilePath.as_posix(), title=title, assets=list())


def _esc_char(match):
    return "\\" + match.group(0)


def _tr_str(name):
    _to_esc = re.compile(r"\s|[]()[]")
    return _to_esc.sub("_", name)


def _escape_str(name):
    _to_esc = re.compile(r"\s|[]()[]")
    return _to_esc.sub(_esc_char, name)


def _init_log(working_dir: Path) -> None:
    log_fp = Path(working_dir, Path("log.txt"))
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


@task(trigger=any_failed)
def copy_workdir_on_fail(working_dir: Path, assets_dir: Path) -> None:
    workd_name = datetime.datetime.now().strftime("work_dir_%I_%M%p_%B_%d_%Y")
    dest = f"{assets_dir.as_posix()}/f{workd_name}"
    log(f"An error occured - will copy {working_dir} to {dest}")
    shutil.copytree(working_dir.as_posix(), dest)


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
            status = "error"
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
def set_up_work_env(input_fp: Path) -> Path:
    """note input"""
    # create a temp space to work
    working_dir = _make_work_dir()
    _init_log(working_dir=working_dir)
    log(f"Working dir for {input_fp} is {working_dir}.")
    return working_dir


@task
def print_t(t):
    """dumb function to print stuff..."""
    log("++++++++++++++++++++++++++++++++++++++++")
    log(t)


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
        raise ValueError(f"Asset type: {asset_type} is not a valid type. {valid_typs}")
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


@task
def send_callback_body(
    token: str,
    callback_url: str,
    files_elts: List[Dict],
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
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = requests.post(callback_url, headers=headers, data=json.dumps(data))
    log(response.url)
    log(response.status_code)
    log(json.dumps(data))
    log(response.text)
    log(response.headers)
