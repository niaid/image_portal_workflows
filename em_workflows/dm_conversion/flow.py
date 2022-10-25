from collections import namedtuple
import datetime
import os
import requests
import json
from pathlib import Path
import prefect
from typing import List, NamedTuple, Optional
from prefect import Flow, task, Parameter, unmapped
from prefect.run_configs import LocalRun
from prefect.engine import signals
from pytools.meta import is_int16
from pytools.convert import file_to_uint8

from em_workflows.file_path import FilePath
from em_workflows.config import Config
from em_workflows.utils import utils


@task
def convert_dms_to_mrc(file_path: FilePath) -> None:
    cur = file_path.current
    if cur.suffix.lower() == ".dm3" or cur.suffix.lower() == ".dm4":
        output_fp = file_path.gen_output_fp(output_ext=".mrc")
        msg = f"Using dir: {cur}, : creating output_fp {output_fp}"
        utils.log(msg=msg)
        log_file = f"{output_fp.parent}/dm2mrc.log"
        cmd = [Config.dm2mrc_loc, cur.as_posix(), output_fp.as_posix()]
        # utils.log(f"Generated cmd {cmd}")
        FilePath.run(cmd=cmd, log_file=log_file)
        file_path.update_current(output_fp)


@task
def convert_if_int16_tiff(file_path: FilePath) -> None:
    """
    accepts a tiff Path obj
    tests if 16 bit
    if 16 bit convert (write to assets_dir) & return Path
    else return orig Path
    """
    # first check if file extension is .tif or .tiff
    if (
        file_path.current.suffix.lower() == ".tiff"
        or file_path.current.suffix.lower() == ".tif"
    ):
        utils.log(f"{file_path.current.as_posix()} is a tiff file")
        if is_int16(file_path.current):
            new_fp = Path(
                f"{file_path.working_dir.as_posix()}/{file_path.current.name}"
            )
            utils.log(
                f"{file_path.current.as_posix()} is a 16 bit tiff, converting to {new_fp}"
            )
            file_to_uint8(in_file_path=file_path.current, out_file_path=str(new_fp))
            file_path.update_current(new_fp=new_fp)


@task
def convert_mrc_to_jpeg(file_path: FilePath) -> None:
    cur = file_path.current
    if cur.suffix == ".mrc":
        output_fp = file_path.gen_output_fp(".jpeg")
        log_fp = f"{output_fp.parent}/mrc2tif.log"
        cmd = [Config.mrc2tif_loc, "-j", cur.as_posix(), output_fp]
        utils.log(f"Generated cmd {cmd}")
        FilePath.run(cmd, log_fp)
        file_path.update_current(output_fp)



@task
def scale_jpegs(file_path: FilePath) -> Optional[dict]:
    """
    generates keyThumbnail and keyImage
    """
    cur = file_path.current
    if file_path.filter_by_suffix([".tif", ".tiff", ".jpeg", ".png", ".jpg"]):
        output_sm = file_path.gen_output_fp("_SM.jpeg")
        output_lg = file_path.gen_output_fp("_LG.jpeg")
        cmd_sm = [
            "gm",
            "convert",
            "-size",
            Config.size_sm,
            cur.as_posix(),
            "-resize",
            Config.size_sm,
            "-sharpen",
            "2",
            "-quality",
            "70",
            output_sm,
        ]
        cmd_lg = [
            "gm",
            "convert",
            "-size",
            Config.size_lg,
            cur.as_posix(),
            "-resize",
            Config.size_lg,
            "-sharpen",
            "2",
            "-quality",
            "70",
            output_lg,
        ]
        log_sm = f"{output_sm.parent}/jpeg_sm.log"
        log_lg = f"{output_lg.parent}/jpeg_lg.log"
        FilePath.run(cmd_sm, log_sm)
        FilePath.run(cmd_lg, log_lg)
        assets_fp_sm = file_path.copy_to_assets_dir(fp_to_cp=output_sm)
        assets_fp_lg = file_path.copy_to_assets_dir(fp_to_cp=output_lg)
        prim_fp = file_path.prim_fp_elt
        prim_fp = file_path.add_asset(
            prim_fp=prim_fp, asset_fp=assets_fp_sm, asset_type="keyThumbnail"
        )

        prim_fp = file_path.add_asset(
            prim_fp=prim_fp, asset_fp=assets_fp_lg, asset_type="keyImage"
        )
        return prim_fp


@task
def create_gm_cmd(fp_in: Path, fp_out: Path, size: str) -> str:
    # gm convert
    # -size 300x300 "/io/20210525_1416_A000_G000.jpeg"
    # -resize 300x300 -sharpen 2 -quality 70 "/io/20210525_1416_A000_G000_sm.jpeg"
    if size == "sm":
        scaler = Config.size_sm
    elif size == "lg":
        scaler = Config.size_lg
    else:
        raise signals.FAIL(f"Thumbnail must be either sm or lg, not {size}")
    cmd = f"gm convert -size {scaler} {fp_in} -resize {scaler} -sharpen 2 -quality 70 {fp_out}"
    utils.log(f"Generated cmd {cmd}")
    return cmd




@task(max_retries=3, retry_delay=datetime.timedelta(minutes=1))
def send_callback(
    token: str,
    callback_url: str,
    callback: dict,
) -> int:
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
    data = json.dumps({"files:": callback})

    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    logger = prefect.context.get("logger")
    logger.info(data)
    response = requests.post(callback_url, headers=headers, data=data)
    logger.info(response.url)
    logger.info(response.status_code)
    logger.info(response.text)
    logger.info(response.headers)
    if response.status_code != 204:
        msg = f"Bad response code on callback: {response}"
        raise ValueError(msg)
    return response.status_code


#@task
#def list_files(input_dir: Path, exts: List[str], single_file: str = None) -> List[Path]:
#    """
#    List all files within input_dir with spefified extension.
#    if a specific file is requested that file is returned only.
#    This allows workflows run on single files rather than entire dirs (default).
#    Note, if no files are found does NOT raise exception. Function can be called
#    multiple times, sometimes there will be no files of that extension.
#    """
#    _files = list()
#    logger = prefect.context.get("logger")
#    logger.info(f"Looking for *.{exts} in {input_dir}")
#    if single_file:
#        fp = Path(f"{input_dir}/{single_file}")
#        ext = fp.suffix.strip(".")
#        if ext in exts:
#            if not fp.exists():
#                raise signals.FAIL(
#                    f"Expected file: {single_file}, not found in input_dir"
#                )
#            else:
#                _files.append(fp)
#    else:
#        for ext in exts:
#            _files.extend(input_dir.glob(f"*.{ext}"))
#    if not _files:
#        raise signals.FAIL(f"Input dir does not contain anything to process.")
#    logger.info("found files")
#    logger.info(_files)
#    return _files


@task
def pint_obj(fp: FilePath) -> None:
    print("tttt")


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



with Flow(
    "dm_to_jpeg",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=Config.SLURM_EXECUTOR,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:
    """
    [dm4 and dm3 inputs] ---> [mrc intermediary files] ---> [jpeg outputs]    -->
                                                            [add jpeg inputs] -->

    --> ALL scaled into sizes "sm" and "lg"
    """
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        Config.valid_2d_input_exts,
        single_file=file_name,
    )
    fps = utils.gen_fps(input_dir=input_dir_fp, fps_in=input_fps)
    # logs = utils.init_log.map(file_path=fps)

    tiffs_converted = convert_if_int16_tiff.map(file_path=fps)

    # dm* to mrc conversion
    dm_to_mrc_converted = convert_dms_to_mrc.map(fps, upstream_tasks=[tiffs_converted])

    # mrc is intermed format, to jpeg conversion
    mrc_to_jpeg = convert_mrc_to_jpeg.map(fps, upstream_tasks=[dm_to_mrc_converted])

    # scale the jpegs, pngs, and tifs
    scaled_jpegs = scale_jpegs.map(fps, upstream_tasks=[mrc_to_jpeg])

    cp_wd_to_assets = utils.copy_workdirs.map(fps, upstream_tasks=[scaled_jpegs])

    callback_sent = send_callback(
        token=token,
        callback_url=callback_url,
        callback=scaled_jpegs,
        upstream_tasks=[cp_wd_to_assets],
    )
