import datetime
import requests
import json
from pathlib import Path
from typing import List, Optional
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
        cmd = [Config.dm2mrc_loc, cur.as_posix(), output_fp]
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
def cleanup_workdir(file_path: FilePath) -> None:
    file_path.rm_workdir()


@task
def copy_workdirs(file_path: FilePath) -> None:
    file_path.copy_workdir_to_assets()


@task
def scale_jpegs(file_path: FilePath) -> None:
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
        file_path.add_assets_entry(asset_path=assets_fp_sm, asset_type="keyThumbnail")
        file_path.add_assets_entry(asset_path=assets_fp_lg, asset_type="keyImage")


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


@task
def gen_fps(projects_dir: Path, fps_in: List[Path]) -> List[FilePath]:
    fps = list()
    for fp in fps_in:
        file_path = FilePath(proj_dir=projects_dir, fp_in=fp)
        msg = f"created working_dir {file_path.working_dir} for {fp.as_posix()}"
        utils.log(msg)
        fps.append(file_path)
    return fps


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=1))
def gen_callback_body(
    token: str,
    callback_url: str,
    files_elts: List[FilePath],
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
    elts = list()
    for fp in files_elts:
        elts.append(fp.prim_fp_elt)
    data = json.dumps({"files:": elts})

    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = requests.post(callback_url, headers=headers, data=data)
    utils.log(response.url)
    utils.log(response.status_code)
    utils.log(data)
    utils.log(response.text)
    utils.log(response.headers)
    if response.status_code != 204:
        msg = f"Bad response code on callback: {response}"
        raise ValueError(msg)


@task
def pint_obj(fp: FilePath) -> None:
    print("tttt")


with Flow(
    "dm_to_jpeg",
    # state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    # executor=Config.SLURM_EXECUTOR,
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
        [
            "DM4",
            "DM3",
            "dm4",
            "dm3",
            "TIF",
            "TIFF",
            "JPEG",
            "PNG",
            "JPG",
            "tif",
            "tiff",
            "jpeg",
            "png",
            "jpg",
        ],
        single_file=file_name,
    )
    utils.check_inputs_ok(input_fps)
    fps = gen_fps(projects_dir=input_dir_fp, fps_in=input_fps)
    logs = utils.init_log.map(file_path=fps)

    tiffs_converted = convert_if_int16_tiff.map(file_path=fps)

    # dm* to mrc conversion
    dm_to_mrc_converted = convert_dms_to_mrc.map(fps, upstream_tasks=[tiffs_converted])

    # mrc is intermed format, to jpeg conversion
    mrc_to_jpeg = convert_mrc_to_jpeg.map(fps, upstream_tasks=[dm_to_mrc_converted])

    # scale the jpegs, pngs, and tifs
    scaled_jpegs = scale_jpegs.map(fps, upstream_tasks=[mrc_to_jpeg])

    callback_sent = gen_callback_body(
        token=token,
        callback_url=callback_url,
        files_elts=fps,
        upstream_tasks=[scaled_jpegs],
    )

    cp_wd_to_assets = copy_workdirs.map(fps, upstream_tasks=[scaled_jpegs])
    cleanup = cleanup_workdir.map(fps, upstream_tasks=[cp_wd_to_assets])
