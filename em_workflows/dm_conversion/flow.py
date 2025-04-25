from pathlib import Path
from typing import Optional

import SimpleITK as sitk
import SimpleITK.utilities as sitkutils

from prefect import flow, task, unmapped, allow_failure
from pytools.meta import is_16bit
from pytools.convert import file_to_uint8

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.constants import AssetType
from em_workflows.dm_conversion.config import DMConfig
from em_workflows.dm_conversion.constants import (
    LARGE_DIM,
    SMALL_DIM,
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


def _newstack_mrc_to_tiff(input_fn: Path, output_fn: Path, log_fn: Path, use_float=True, verbose=False) -> None:
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
    :param verbose: If true, the newstack command is run in normal mode. Otherwise, quiet mode is enabled.
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

    if not verbose:
        cmd.append("-quiet")

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
    name="SITK Scale Jpeg",
    on_failure=[utils.collect_exception_task_hook],
)
def sitk_scale_jpegs(file_path: FilePath) -> (dict, dict):
    """
    generates keyThumbnail and keyImage


    """

    convert_em_to_tiff.fn(file_path)

    small_size = (SMALL_DIM, )*2
    large_size = (LARGE_DIM, )*2

    as_tiff = Path(f"{file_path.working_dir}/{file_path.base}_as_tiff.tiff")
    if as_tiff.exists():
        cur = as_tiff
    elif file_path.fp_in.suffix.strip(".").lower() in KEYIMG_EXTS:
        cur = file_path.fp_in
    else:
        msg = f"Impossible state for {file_path.fp_in}"
        raise RuntimeError(msg)

    output_small = file_path.gen_output_fp(output_ext="_SM.jpeg")

    utils.log(msg=f"Reading {cur}...")
    img = sitk.ReadImage(cur)
    utils.log(msg=f"Generating {img.GetSize()}->{small_size} {output_small}...")
    small_img = sitkutils.resize(img, small_size, sitk.sitkLinear)
    sitk.WriteImage(small_img, output_small, useCompression=True, compressionLevel=70)
    asset_type = AssetType.THUMBNAIL
    asset_small_fp = file_path.copy_to_assets_dir(fp_to_cp=output_small)
    asset_small_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_small_fp)

    output_large = file_path.gen_output_fp(output_ext="_LG.jpeg")

    utils.log(msg=f"Generating {img.GetSize()}->{large_size} {output_large}...")
    large_img = sitkutils.resize(img, large_size, sitk.sitkLinear)
    sitk.WriteImage(large_img, output_large, useCompression=True, compressionLevel=80)

    asset_type = AssetType.KEY_IMAGE
    asset_large_fp = file_path.copy_to_assets_dir(fp_to_cp=output_large)
    asset_large_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_large_fp)

    return utils.gen_prim_fps.fn(fp_in=file_path, additional_assets=(asset_small_elt, asset_large_elt))


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

    prim_fps = sitk_scale_jpegs.map( fps )

    callback_result = list()
    for idx, (fp, cb) in enumerate(zip(fps.result(), prim_fps)):
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
