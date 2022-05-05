import requests
import os
import shutil
import json
import prefect
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
def add_assets(assets_list: Dict, new_asset: Dict[str, str]) -> Dict:
    assets_list.get("assets").append(new_asset)
    return assets_list


@task
def generate_callback_files(input_fname: Path, input_fname_b: Path = None) -> Dict:
    """
    creates the base part callback, to which assets can be added.
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
    """Does nothing more than concat the POSTed input file path to the
    mount point.
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

@task
def to_command(cmd_and_fp: List[str]) -> str:
    return cmd_and_fp[0]

@task
def to_fp(cmd_and_fp: List[str]) -> Optional[Path]:
    """
    checks that an output file exists,
    returns a Path for that file.
    """
    path = Path(cmd_and_fp[1])
    if path.exists():
        return path
    else:
        raise signals.FAIL(f"File {cmd_and_fp[1]} does not exist.")

@task
def wrapper(cmd_and_fp: List[str], asset_dir: Path, asset_type: str):
    """
    placeholder for refactor
    """
    fp = to_fp(cmd_and_fp=cmd_and_fp)
    asset_fp = copy_to_assets_dir(fp=fp, assets_dir=asset_dir)
    loc_elt = gen_assets_entry( asset_type=asset_type, path=asset_fp)
    return loc_elt

@task
def gen_assets_entry(
    path: Path, asset_type: str, metadata: Dict[str, str] = None
) -> Dict[str, str]:
    """
    asset type can be one of:

    Thumbnail
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
        "Thumbnail",
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
def make_assets_dir(input_dir: str) -> Path:
    """
    input_dir comes in the form RMLEMHedwigQA/Projects/Lab/PI/
    want to create: {mount_point}/RMLEMHedwigQA/Assets/Lab/PI/
    """
    input_dir = input_dir.replace("/Projects/", "/Assets/")
    if not input_dir.endswith("/"):
        input_dir = input_dir + "/"
    assets_dir = Path(f"{Config.mount_point}{input_dir}")
    logger = prefect.context.get("logger")
    logger.info(f"making assets dir for {input_dir} at {assets_dir.as_posix()}")
    assets_dir.mkdir(parents=True, exist_ok=True)
    return assets_dir


@task
def copy_to_assets_dir(fp: Path, assets_dir: Path) -> Path:
    """
    copy desired outputs to the assets (reported output) dir
    eg copy /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/SARsCoV2_1/keyMov_SARsCoV2_1.mp4
    to
    /mnt/ai-fas12/RMLEMHedwigQA/Assets/Lab/Pi/SARsCoV2_1/keyMov_SARsCoV2_1.mp4
    {mount_point}/{input_dir_as_asset}
    eg temp_dir = /gs1/home/macmenaminpe/tmp/tmpgfcvuqz0/SARsCoV2_1
    want to copy /gs1/home/macmenaminpe/tmp/tmpgfcvuqz0 (ie temp_dir.parent)
    (note keep SARsCoV2_1) to assets_dir
    """
    # full_fp = Path(f"{Config.proj_dir}
    # want to remove the temp path, up until the tg name dir
    tempdir_no_tg_name = fp.parent.parent
    # eg SARsCoV2_1/keyMov_SARsCoV2_1.mp4
    name_dir_fp = tempdir_no_tg_name.relative_to(fp)
    dest = Path(f"{assets_dir}/{name_dir_fp}")
    shutil.copytree(fp, dest)
    # API doesn't want to know about mount_point
    return dest.relative_to(Config.mount_point)

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
    if not input_dir.endswith("/"):
        input_dir = input_dir + "/"
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
