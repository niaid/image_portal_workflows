from pathlib import Path
from typing import Optional

from prefect import flow, task, unmapped, allow_failure
from pytools.meta import is_16bit
from pytools.convert import file_to_uint8

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.constants import LARGE_DIM, AssetType
from em_workflows.dm_conversion.config import DMConfig
from em_workflows.dm_conversion.constants import (
    LARGE_2D,
    SMALL_2D,
    TIFS_EXT,
    DMS_EXT,
    MRCS_EXT,
    KEYIMG_EXTS,
    VALID_2D_INPUT_EXTS,
)


def _calculate_shrink_factor(filepath: Path, enforce_2d: bool = True, target_size: int = LARGE_DIM) -> float:
    """
    Calculate the shrink factor for the newstack command
    """
    dims = utils.lookup_dims(filepath)

    if enforce_2d and dims.z != 1:
        msg = f"mrc file {filepath} is not 2 dimensional. Contains {dims.z} Z dims."
        raise RuntimeError(msg)

    # use the min dimension of x & y to compute shrink_factor
    min_xy = min(dims.x, dims.y)
    # work out shrink_factor to make the resulting image LARGE_DIM
    return min_xy / target_size


def _newstack_mrc_to_tiff(input_fn: Path, output_fn: Path, log_fn: Path, use_float=True) -> None:
    """
    Convert mrc files to tiff using newstack while reducing the size of the image, and performing antialiasing suitable
    for EM images.

    https://bio3d.colorado.edu/imod/doc/man/newstack.html

    :param input_fn: Input file path of a mrc file.
    :param output_fn: Output file path of a tiff file.
    :param log_fn: File path to a log file for the newstack command output.
    :param use_float: If true uses the newstack "float" option to  fill the data range of the output. This range is
      computed after the shrink operation. If false, uses the "meansd" option with the default values of 140,50, which
      computed before the shrink operation.
    :return:
    """

    shrink_factor = _calculate_shrink_factor(input_fn, target_size=LARGE_DIM)

    cmd = [
        DMConfig.newstack_loc,
        "-shrink",
        f"{shrink_factor:.3f}",
        "-antialias",
        "6",
        "-mode",
        "0",
        ]

    if use_float:
        cmd.extend(["-float", "1"])
    else:
        cmd.extend(["-meansd", "140,50"])

    cmd.extend([
        input_fn.as_posix(),
        output_fn.as_posix(),
        ])

    FilePath.run(cmd, str(log_fn), env={"IMOD_OUTPUT_FORMAT": "TIF"} )

@task(
    name="Convert EM images to tiff",
    on_failure=[utils.collect_exception_task_hook],
)
def convert_em_to_tiff(file_path: FilePath) -> FilePath:

    out_fp = file_path.gen_output_fp(output_ext="_as_tiff.tiff")
    log_fp = file_path.gen_output_fp(output_ext="_as_tiff.log")

    if file_path.fp_in.suffix.strip(".").lower() in MRCS_EXT:

        utils.log(f"{file_path.fp_in.as_posix()} is a mrc file, will convert to {out_fp}.")

        _newstack_mrc_to_tiff(file_path.fp_in, out_fp, log_fp, use_float=False)

    elif (
            file_path.fp_in.suffix.strip(".").lower() in TIFS_EXT
            and is_16bit(file_path.fp_in)
    ):

        utils.log(f"{file_path.fp_in} is a 16 bit tiff, converting to {out_fp}")

        _newstack_mrc_to_tiff(file_path.fp_in, out_fp, log_fp, use_float=True)

    elif file_path.fp_in.suffix.strip(".").lower() in DMS_EXT:
        dm_as_mrc = file_path.gen_output_fp(output_ext="dm_as_mrc.mrc")
        utils.log(msg=f"{file_path.fp_in} is a dm file, will convert to {dm_as_mrc}.")

        mrc_log_fp = file_path.gen_output_fp(output_ext="_dm2mrc.log")

        cmd = [DMConfig.dm2mrc_loc, file_path.fp_in.as_posix(), dm_as_mrc.as_posix()]
        FilePath.run(cmd=cmd, log_file=str(mrc_log_fp))

        utils.log(f"{dm_as_mrc} convert to {out_fp}.")

        _newstack_mrc_to_tiff(dm_as_mrc, out_fp, log_fp, use_float=True)

    return file_path


@task(
    name="Scale Jpeg",
    on_failure=[utils.collect_exception_task_hook],
)
def scale_jpegs(file_path: FilePath, size: str) -> Optional[dict]:
    """
    generates keyThumbnail and keyImage
    looks for file names <something>_mrc_as_jpg.jpeg
    """

    as_tiff = Path(f"{file_path.working_dir}/{file_path.base}_as_tiff.tiff")
    if as_tiff.exists():
        cur = as_tiff
    elif file_path.fp_in.suffix.strip(".").lower() in KEYIMG_EXTS:
        cur = file_path.fp_in
    else:
        msg = f"Impossible state for {file_path.fp_in}"
        raise RuntimeError(msg)

    if size.lower() == "s":
        output = file_path.gen_output_fp("_SM.jpeg")
        log = f"{output.parent}/jpeg_sm.log"
        asset_type = AssetType.THUMBNAIL
        cmd = [
            DMConfig.gm_loc,
            "convert",
            "-size",
            SMALL_2D,
            cur.as_posix(),
            "-resize",
            SMALL_2D,
            "-sharpen",
            "2",
            "-quality",
            "70",
            output.as_posix(),
        ]
    elif size.lower() == "l":
        output = file_path.gen_output_fp("_LG.jpeg")
        log = f"{output.parent}/jpeg_lg.log"
        asset_type = AssetType.KEY_IMAGE
        cmd = [
            DMConfig.gm_loc,
            "convert",
            "-size",
            LARGE_2D,
            cur.as_posix(),
            "-resize",
            LARGE_2D,
            # "-sharpen",
            # "2",
            "-quality",
            "80",
            output.as_posix(),
        ]
    else:
        msg = "Jpeg scaler must have size argument set to 'l' or 's'"
        raise RuntimeError(msg)
    FilePath.run(cmd, log)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=output)
    asset_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_fp)
    return asset_elt


@flow(
    name="Small 2D",
    flow_run_name=utils.generate_flow_run_name,
    log_prints=True,
    task_runner=DMConfig.SLURM_EXECUTOR,
    on_completion=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_failure=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_crashed=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
)
def dm_flow(
    file_share: str,
    input_dir: str,
    x_file_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    token: Optional[str] = None,
    x_no_api: bool = False,
    x_keep_workdir: bool = False,
):
    # run_config=LocalRun(labels=[utils.get_environment()]),
    """
    -list all inputs (ie files of relevant input type)
    -create output dir for each.
    -convert all 16bit tiffs to 8 bit, write to Assets dir.
    -convert all dm3/dm4 files to mrc, write to Assets dir.
    -convert <name>dm_as_mrc.mrc files to jpegs.
    -convert all mrcs in Projects dir to jpegs.
    -convert all tiffs/pngs/jpegs to correct size for thumbs, "sm" and "lg"
    """
    utils.notify_api_running(x_no_api, token, callback_url)

    # utils.log(input_dir)
    input_dir_fp = utils.get_input_dir.submit(
        share_name=file_share, input_dir=input_dir
    )

    input_fps = utils.list_files.submit(
        input_dir_fp,
        VALID_2D_INPUT_EXTS,
        single_file=x_file_name,
    )

    fps = utils.gen_fps.submit(
        share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps
    )

    tiff_results = convert_em_to_tiff.map(file_path=fps)

    # Finally generate all valid suffixed results
    keyimg_assets = scale_jpegs.map(
        fps,
        size=unmapped("l"),
        wait_for=[allow_failure(tiff_results), allow_failure(tiff_results)],
    )
    thumb_assets = scale_jpegs.map(
        fps,
        size=unmapped("s"),
        wait_for=[allow_failure(tiff_results), allow_failure(tiff_results)],
    )

    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_keyimgs = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=keyimg_assets
    )

    callback_result = list()
    for idx, (fp, cb) in enumerate(zip(fps.result(), callback_with_keyimgs)):
        state = cb.wait()
        if state.is_completed():
            callback_result.append(cb.result())
        else:
            path = f"{state.state_details.flow_run_id}__{idx}"
            try:
                message = DMConfig.local_storage.read_path(path)
                callback_result.append(fp.gen_prim_fp_elt(message.decode()))
            except ValueError:
                callback_result.append(fp.gen_prim_fp_elt("Something went wrong!"))

    utils.send_callback_body.submit(
        x_no_api=x_no_api,
        token=token,
        callback_url=callback_url,
        files_elts=callback_result,
    )
