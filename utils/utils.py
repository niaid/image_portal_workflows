import tempfile
import requests
import os
import shutil
import json
import prefect
import glob
from typing import List, Dict, Optional
from pathlib import Path
from prefect import task, context
from prefect import Flow, task, context
from prefect.engine.state import State
from prefect.engine import signals

from prefect.triggers import all_finished
from image_portal_workflows.config import Config

logger = context.get("logger")


@task
def make_work_dir(fname: Path = None) -> Path:
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
    prefect.context.get("logger").info(msg)
    return Path(working_dir)


@task
def create_brt_command(adoc_fp: Path) -> str:
    cmd = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1"
    logger = prefect.context.get("logger")
    logger.info(cmd)
    return cmd
    # to short test
    # return "ls"


@task
def list_input_dir(input_dir_fp: Path) -> List[Path]:
    """
    discover the contents of the input_dir AKA "Sample"
    note, only lists mrc files currently. TODO(?)
    include .st files TODO
    note, only processing first file ATM (test)
    """
    logger = prefect.context.get("logger")
    logger.info(f"trying to list {input_dir_fp}")
    mrc_files = glob.glob(f"{input_dir_fp}/*.mrc")
    if len(mrc_files) == 0:
        raise signals.FAIL(f"Unable to find any input files in dir: {input_dir_fp}")
    mrc_fps = [Path(f) for f in mrc_files]
    # TESTING IGNORE
    # mrc_fps = [Path(f"/home/macmenaminpe/data/brt_run/Projects/ABC2/2013-1220-dA30_5-BSC-1_10.mrc")]
    logger = prefect.context.get("logger")
    logger.info(f"Found {mrc_fps}")
    return mrc_fps


@task
def add_assets(assets_list: Dict, new_asset: Dict[str, str]) -> Dict:
    logger = context.get("logger")
    logger.info(f"Trying to add asset {new_asset}")
    assets_list.get("assets").append(new_asset)
    return assets_list


@task
def generate_callback_files(input_fname: Path, input_fname_b: Path = None) -> Dict:
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
    primaryFilePath = _clean_subdir(subdir=Config.proj_dir, fp=input_fname)
    return dict(primaryFilePath=primaryFilePath.as_posix(), title=title, assets=list())


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
    # _file_names = [Path(_file.name) for _file in _files]
    logger.info("Found files:")
    logger.info(_files)
    return _files


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
def gen_output_fp(input_fp: Path, output_ext: str, working_dir: Path = None) -> Path:
    """
    cat working_dir to input_fp.name, but swap the extension to output_ext
    the reason for having a working_dir default to None is sometimes the output
    dir is not the same as the input dir, and working_dir is used to define output
    in this case.
    """
    if working_dir:
        output_fp = f"{working_dir.as_posix()}/{input_fp.stem}{output_ext}"
    else:
        output_fp = f"{input_fp.parent}/{input_fp.stem}{output_ext}"
    prefect.context.get("logger").info(
        f"Using dir: {working_dir}, file: {input_fp}, ext: {output_ext} creating output_fp {output_fp}"
    )
    return Path(output_fp)


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
        callback_url = prefect.context.parameters.get("callback_url")
        token = prefect.context.parameters.get("token")
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
    Copies inputs, as defined as files with the appropriate extensions, from input dir
    to output_dir (where they will be processed).
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
    """
    Concat the POSTed input file path to the mount point.
    returns Path obj
    """
    input_path = Path(Config.proj_dir + input_dir)
    logger.info(f"Input path is {input_path}")
    return input_path


@task
def clean_up_outputs_dir(assets_dir: Path, to_keep: List[Dict], trigger=all_finished):
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


def _clean_subdir(subdir: str, fp: Path) -> Path:
    """
    gets rid of a leading subdir from from path
    eg /to/junk/the/path/fname -> /the/path/fname
    note - path.relative_to raises a ValueError if it
    does not contain the subpath, thus the strange looking try.
    """
    try:
        if fp.relative_to(subdir):
            fp = fp.relative_to(Config.proj_dir)
        return fp
    except ValueError:
        return fp


#@task
#def to_command(cmd_and_fp: List[str]) -> str:
#    return cmd_and_fp[0]
#

#def _to_fp(cmd_and_fp: List[str]) -> Optional[Path]:
#    """
#    checks that an output file exists,
#    returns a Path for that file.
#    """
#    path = Path(cmd_and_fp[1])
#    if path.exists():
#        return path
#    else:
#        raise signals.FAIL(f"File {cmd_and_fp[1]} does not exist.")
#

#@task
#def move_to_assets(
#    cmd_and_fp: List[str],
#    asset_dir: Path,
#    asset_type: str,
#    input_fp: Path,
#    metadata: Dict = None,
#):
#    """
#    extracts the asset location from list
#    moves that file to assets dir
#    generates
#    placeholder for refactor
#    """
#    logger = prefect.context.get("logger")
#    fp = _to_fp(cmd_and_fp=cmd_and_fp)
#    logger.info(f"Trying to move {fp}")
#    asset_fp = _move_to_assets_dir(fp=fp, assets_dir=asset_dir, dname=input_fp.stem)
#    loc_elt = _gen_assets_entry(asset_type=asset_type, path=asset_fp, metadata=metadata)
#    return loc_elt


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
    fp_no_mount_point = path.relative_to(Config.mount_point)
    if metadata:
        asset = {asset_type: fp_no_mount_point.as_posix(), "metadata": metadata}
    else:
        asset = {asset_type: fp_no_mount_point.as_posix()}
    return asset
    # base_elt["assets"].append(asset)


def _gen_assets_entry(
    path: Path, asset_type: str, metadata: Dict[str, str] = None
) -> Dict[str, str]:
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
    """
    valid_typs = [
        "averagedVolume",
        "keyImage",
        "keyThumbnail",
        "recMovie",
        "tiltMovie",
        "volume",
        "neuroglancerPrecomputed",
    ]
    if asset_type not in valid_typs:
        raise ValueError(f"Asset type: {asset_type} is not a valid type. {valid_typs}")
    if metadata:
        asset = {asset_type: path.as_posix(), "metadata": metadata}
    else:
        asset = {asset_type: path.as_posix()}
    return asset


@task
def make_assets_dir(input_dir: Path) -> Path:
    """
    input_dir comes in the form {mount_point}/RMLEMHedwigQA/Projects/Lab/PI/
    want to create: {mount_point}/RMLEMHedwigQA/Assets/Lab/PI/
    """
    if not "Projects" in input_dir.as_posix():
        raise signals.FAIL(f"Input directory {input_dir} does not contain Projects")
    assets_dir_as_str = input_dir.as_posix().replace("/Projects/", "/Assets/")
    assets_dir = Path(assets_dir_as_str)
    prefect.context.get("logger").info(
        f"making assets dir for {input_dir} at {assets_dir.as_posix()}"
    )
    assets_dir.mkdir(parents=True, exist_ok=True)
    return assets_dir


@task
def _move_to_assets_dir(fp: Path, assets_dir: Path, prim_fp: Path) -> Path:
    """
    Move desired outputs to the assets (reported output) dir
    eg copy /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/keyMov_SARsCoV2_1.mp4
    to
    /mnt/ai-fas12/RMLEMHedwigQA/Assets/Lab/Pi/SARsCoV2_1/keyMov_SARsCoV2_1.mp4
    {mount_point}/{dname}/keyMov_SARsCoV2_1.mp4
    (note dname SARsCoV2_1) in assets_dir
    """
    logger = prefect.context.get("logger")
    assets_sub_dir = Path(f"{assets_dir}/{prim_fp.stem}")
    assets_sub_dir.mkdir(exist_ok=True)
    dest = Path(f"{assets_sub_dir}/{fp.name}")
    logger.info(f"Trying to copy {fp} to {dest}")
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
    logger.info(response.url)
    logger.info(response.status_code)
    logger.info(json.dumps(data))
    logger.info(response.text)
    logger.info(response.headers)
