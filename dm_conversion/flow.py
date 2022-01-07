import requests
import os
import shutil
import json
from typing import Optional, List, Dict
from pathlib import Path
import prefect
from prefect.engine import signals
from prefect import Flow, task, Parameter, unmapped, context
from prefect.engine.state import State
from prefect.tasks.docker.containers import (
    CreateContainer,
    StartContainer,
    WaitOnContainer,
    GetContainerLogs,
)


from image_portal_workflows.config import Config

logger = context.get("logger")


class Job:
    def __init__(self, input_dir):
        self.output_dir = Path(Config.assets_dir + input_dir)
        self.input_dir = Path(Config.proj_dir + input_dir)


class Container_Dm2mrc(CreateContainer):
    def run(self, input_dir: Path, fp: Path, output_fp: Path):
        logger.info(f"Dm2mrc mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir.as_posix() + ":/io"]},
            command=[Config.dm2mrc_loc, f_in, f_out],
        )


class Container_gm_convert(CreateContainer):
    gm = "/usr/bin/gm"
    sharpen = "2"
    # docker run
    # -v $(pwd)/test/input_files/:/io
    # graphicsmagick gm convert
    # -size 300x300 "/io/20210525_1416_A000_G000.jpeg"
    # -resize 300x300 -sharpen 2 -quality 70 "/io/20210525_1416_A000_G000_sm.jpeg"

    def run(self, input_dir: Path, fp: Path, output_fp: Path, size: str):
        logger.info(f"gm_convert mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        if size == "sm":
            scaler = Config.size_sm
        elif size == "lg":
            scaler = Config.size_lg
        else:
            raise signals.FAIL(f"Thumbnail must be either sm or lg, not {size}")
        return super().run(
            image_name="graphicsmagick",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir.as_posix() + ":/io"]},
            command=[
                Container_gm_convert.gm,
                "convert",
                "-size",
                scaler,
                f_in,
                "-resize",
                scaler,
                "-sharpen",
                "2",
                "-quality",
                "70",
                f_out,
            ],
        )


class Container_Mrc2tif(CreateContainer):
    def run(self, input_dir: Path, fp: Path, output_fp: Path):
        logger.info(f"Mrc2tiff mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir.as_posix() + ":/io"]},
            command=[
                Config.mrc2tif_loc,
                "-j",
                f_in,
                f_out,
            ],
        )


create_mrc = Container_Dm2mrc()
create_jpeg = Container_Mrc2tif()
create_thumb = Container_gm_convert()

startDM = StartContainer()
startMRC = StartContainer()
startGM = StartContainer()
startGMlg = StartContainer()
waitDM = WaitOnContainer()
waitMRC = WaitOnContainer()
waitGMlg = WaitOnContainer()
waitGM = WaitOnContainer()


@task
def list_files(input_dir: Path, ext: str) -> List[Path]:
    _files = list(input_dir.glob(f"**/*.{ext}"))
    _file_names = [Path(_file.name) for _file in _files]
    if not _files:
        raise ValueError(f"{input_dir} contains no files with extension {ext}")
    return _file_names


@task
def init_job(input_dir: Path) -> Job:
    return Job(input_dir=input_dir)


@task
def gen_output_fname(input_fp: Path, output_ext) -> Path:
    output_fp = Path(f"{input_fp.stem}{output_ext}")
    logger.info(f"input: {input_fp} output: {output_fp}")
    return output_fp


@task
def check_input_fname(input_fps: List[Path], fp_to_check: str) -> List[Path]:
    """ensures that a file_name is extant,
    if so will return as Path, else raise exception."""
    if fp_to_check is None:
        return input_fps
    for _fp in input_fps:
        if _fp.name == fp_to_check:
            return [Path(fp_to_check)]
    raise signals.FAIL(f"Expecting file: {fp_to_check}, not found in input_dir")


def _gen_files(dname: str, inputs: List[Path]) -> List[Dict]:
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


def _add_outputs(
    dname: str, files: List[Dict], outputs: List[Path], _type: str
) -> List[Dict]:
    for i, elt in enumerate(files):
        elt["assets"].append({"type": _type, "path": dname + outputs[i].as_posix()})
    return files


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
        #            if environ.get("LOCAL_JOB"):
        #                return new_state
        if new_state.is_successful():
            status = "success"
        else:
            status = "fail"
    return new_state


@task
def generate_callback_body(
    sample_id: str,
    token: str,
    callback_url: str,
    input_dir,
    inputs,
    jpeg_locs,
    small_thumb_locs,
):
    """
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
    files = _gen_files(dname=input_dir, inputs=inputs)
    files = _add_outputs(
        dname=input_dir, files=files, outputs=jpeg_locs, _type="keyImage"
    )
    files = _add_outputs(
        dname=input_dir, files=files, outputs=small_thumb_locs, _type="thumbnail"
    )
    data = {"status": "success", "files": files}
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = requests.post(callback_url, headers=headers, data=json.dumps(data))
    logger.info(response.url)
    logger.info(response.status_code)
    logger.info(json.dumps(data))
    logger.info(response.text)
    logger.info(response.headers)


@task
def copy_inputs_to_outputs_dir(
    input_dir_fp: Path, output_dir_fp: Path, fps: List[Path]
):
    """
    inputs are found in /Projects/Lab/PI/Proj_name/Session_name/Sample_name/
    outputs are placed in /Assets/Lab/PI/Proj_name/Session_name/Sample_name/
    I'm going to copy inputs over, and process them in place.
    """
    for fp in fps:
        full_fp = f"{input_dir_fp}/{fp}"
        logger.info(f"coping {full_fp} to {output_dir_fp}")
        shutil.copy(full_fp, output_dir_fp)


@task
def gen_output_dir(input_dir: str) -> Path:
    output_path = Path(Config.assets_dir + input_dir)
    logger.info(f"Output path is {output_path}")
    os.makedirs(output_path.as_posix(), exist_ok=True)
    return output_path


@task
def get_input_dir(input_dir: str) -> Path:
    input_path = Path(Config.proj_dir + input_dir)
    logger.info(f"Input path is {input_path}")
    return input_path


with Flow("dm_to_jpeg", state_handlers=[notify_api_completion]) as flow:
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url")
    token = Parameter("token")
    sample_id = Parameter("sample_id")
    input_dir_fp = get_input_dir(input_dir=input_dir)
    # job = init_job(input_dir=input_dir)
    dm4_fps = list_files(input_dir_fp, "dm4")
    dm4_fps = check_input_fname(input_fps=dm4_fps, fp_to_check=file_name)
    output_dir_fp = gen_output_dir(input_dir=input_dir)
    copied = copy_inputs_to_outputs_dir(
        input_dir_fp=input_dir_fp, output_dir_fp=output_dir_fp, fps=dm4_fps
    )

    #    # dm* to mrc conversion
    mrc_locs = gen_output_fname.map(input_fp=dm4_fps, output_ext=unmapped(".mrc"))
    mrc_ids = create_mrc.map(
        input_dir=unmapped(output_dir_fp),
        fp=dm4_fps,
        output_fp=mrc_locs,
    )
    mrc_starts = startDM.map(mrc_ids)
    mrc_statuses = waitDM.map(mrc_ids)
    #
    # mrc to jpeg conversion
    jpeg_locs = gen_output_fname.map(input_fp=mrc_locs, output_ext=unmapped(".jpeg"))
    jpeg_container_ids = create_jpeg.map(
        input_dir=unmapped(output_dir_fp),
        fp=mrc_locs,
        output_fp=jpeg_locs,
        upstream_tasks=[mrc_statuses, mrc_starts],
    )
    jpeg_container_starts = startMRC.map(jpeg_container_ids)
    jpeg_status_codes = waitMRC.map(jpeg_container_ids)

    # size down jpegs for small thumbs
    small_thumb_locs = gen_output_fname.map(
        input_fp=jpeg_locs, output_ext=unmapped("_SM.jpeg")
    )
    thumb_container_ids_sm = create_thumb.map(
        input_dir=unmapped(output_dir_fp),
        fp=jpeg_locs,
        output_fp=small_thumb_locs,
        size=unmapped("sm"),
        upstream_tasks=[jpeg_status_codes],
    )
    thumb_container_starts_sm = startGM.map(thumb_container_ids_sm)
    thumb_status_codes_sm = waitGM.map(thumb_container_ids_sm)

    generate_callback_body(
        sample_id,
        token,
        callback_url,
        input_dir,
        dm4_fps,
        jpeg_locs,
        small_thumb_locs,
        upstream_tasks=[thumb_container_starts_sm],
    )
#
#    # size dow jpegs for large thumbs
#    large_thumb_locs = gen_output_fname.map(
#        input_fp=jpeg_locs, output_ext=unmapped("_LG.jpeg")
#    )
#    thumb_container_ids_lg = create_thumb.map(
#        input_dir=unmapped(input_dir),
#        fp=jpeg_locs,
#        output_fp=large_thumb_locs,
#        size=unmapped("lg"),
#        upstream_tasks=[jpeg_status_codes],
#    )
#    thumb_container_starts_lg = startGMlg.map(thumb_container_ids_lg)
#    thumb_status_codes_lg = waitGMlg.map(
#        thumb_container_ids_lg, upstream_tasks=[thumb_container_starts_lg]
#    )
# logs = logs(_id, upstream_tasks=[status_code])
