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


def _calculate_shrink_factor(filepath: Path, enforce_2d=True) -> float:
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
    return min_xy / LARGE_DIM


@task(
    name="Convert DM to MRC",
    on_failure=[utils.collect_exception_task_hook],
)
def convert_dms_to_mrc(file_path: FilePath) -> FilePath:
    if file_path.fp_in.suffix.strip(".").lower() not in DMS_EXT:
        return file_path
    dm_as_mrc = file_path.gen_output_fp(out_fname="dm_as_mrc.mrc")
    msg = f"Using dir: {file_path.fp_in}, : creating dm_as_mrc {dm_as_mrc}"
    utils.log(msg=msg)
    log_file = f"{dm_as_mrc.parent}/dm2mrc.log"
    cmd = [DMConfig.dm2mrc_loc, file_path.fp_in.as_posix(), dm_as_mrc.as_posix()]
    # utils.log(f"Generated cmd {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    return file_path


@task(
    name="Convert 16bit to 8bit tiffs",
    on_failure=[utils.collect_exception_task_hook],
)
def convert_if_int16_tiff(file_path: FilePath) -> None:
    """
    accepts a tiff Path obj
    tests if 16 bit
    if 16 bit convert (write to assets_dir) & return Path
    else return orig Path
    """
    if not (
            file_path.fp_in.suffix.strip(".").lower() in TIFS_EXT
            and is_16bit(file_path.fp_in)
    ):
        return
    tif_8_bit = file_path.gen_output_fp(out_fname="as_8_bit.tif")
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_8_bit.log"
    utils.log(f"{file_path.fp_in} is a 16 bit tiff, converting to {tif_8_bit}")

    shrink_factor = _calculate_shrink_factor(file_path.fp_in)

    cmd = [
        DMConfig.newstack_loc,
        "-shrink",
        f"{shrink_factor:.3f}",
        "-antialias",
        "6",
        "-mode",
        "0",
        # From the newstack manual for "-float": Enter 1 for each section to fill the data range.
        "-float",
        "1",
        file_path.fp_in.as_posix(),
        str(tif_8_bit),
    ]
    utils.log(f"Generated cmd {cmd}")
    FilePath.run(cmd, log_fp, env={"IMOD_OUTPUT_FORMAT": "TIF"})



@task(
    name="Convert 2D MRC to tiff",
    on_failure=[utils.collect_exception_task_hook],
)
def convert_2d_mrc_to_tiff(file_path: FilePath) -> None:
    """
    Checks Projects dir for mrc inputs. We assume anything in Projects will be 2D.
    Converts to tiff file, 1024 in size (using constant LARGE_DIM)

    env IMOD_OUTPUT_FORMAT=TIF newstack \
            --shrink $shrink_factor$ -antialias 6 -mode 0 -meansd 140,50 f_in.mrc f_out.tif
    where shrink_factor is input_size/1024
    used the smaller of x and y (4092/1024)
    """
    # nifti_fp = neuroglancer.gen_niftis.__wrapped__(file_path)
    # utils.log(f"+++++++++++++++++++++++++++++++++++++++++++++")
    # min_max_histo = neuroglancer.gen_min_max_histo(file_path)
    # utils.log(min_max_histo)
    # utils.log(f"+++++++++++++++++++++++++++++++++++++++++++++")

    # work out shrink_factor
    if file_path.fp_in.suffix.strip(".").lower() not in MRCS_EXT:
        return
    shrink_factor = _calculate_shrink_factor( file_path.fp_in)
    # round to 3 decimal places
    shrink_factor_3 = f"{shrink_factor:.3f}"
    out_fp = f"{file_path.working_dir}/{file_path.base}_mrc_as_tiff.tiff"
    log_fp = f"{file_path.working_dir}/{file_path.base}_mrc_as_tiff.log"
    utils.log(f"{file_path.fp_in.as_posix()} is a mrc file, will convert to {out_fp}.")
    # work out meansd
    cmd = [
        DMConfig.newstack_loc,
        "-shrink",
        shrink_factor_3,
        "-antialias",
        "6",
        "-mode",
        "0",
        "-meansd",
        "140,50",
        file_path.fp_in.as_posix(),
        out_fp,
    ]
    utils.log(f"Generated cmd {cmd}")
    FilePath.run(cmd, log_fp, env={"IMOD_OUTPUT_FORMAT": "TIF"})


@task(
    name="Convert DM MRC to jpeg",
    on_failure=[utils.collect_exception_task_hook],
)
def convert_dm_mrc_to_jpeg(file_path: FilePath) -> None:
    """
    converts previously generated mrc file, (derived from a dm file) to jpeg.
    note, ignores everything that's NOT <name>dm_as_mrc.mrc
    that is, convert_dm_mrc_to_jpeg does not process ALL mrc files.
    """
    dm_as_mrc = file_path.gen_output_fp(out_fname="dm_as_mrc.mrc")
    if dm_as_mrc.exists():
        mrc_as_jpg = file_path.gen_output_fp(out_fname="mrc_as_jpg.jpeg")
        log_fp = f"{mrc_as_jpg.parent}/mrc2tif.log"
        cmd = [DMConfig.mrc2tif_loc, "-j", dm_as_mrc.as_posix(), mrc_as_jpg.as_posix()]
        utils.log(f"Generated cmd {cmd}")
        FilePath.run(cmd, log_fp)


@task(
    name="Scale Jpeg",
    on_failure=[utils.collect_exception_task_hook],
)
def scale_jpegs(file_path: FilePath, size: str) -> Optional[dict]:
    """
    generates keyThumbnail and keyImage
    looks for file names <something>_mrc_as_jpg.jpeg
    """
    mrc_as_jpg = file_path.gen_output_fp(out_fname="mrc_as_jpg.jpeg")
    tif_8_bit = file_path.gen_output_fp(out_fname="as_8_bit.tif")
    mrc_as_tiff = Path(f"{file_path.working_dir}/{file_path.base}_mrc_as_tiff.tiff")
    if mrc_as_jpg.exists():
        cur = mrc_as_jpg
    elif tif_8_bit.exists():
        cur = tif_8_bit
    elif mrc_as_tiff.exists():
        cur = mrc_as_tiff  # can ignore here if scaled
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

    # TODO move the three conversion block into if-else based on filetypes
    tiff_results = convert_if_int16_tiff.map(file_path=fps)

    # mrc is intermed format, to jpeg conversion
    dm_to_mrc = convert_dms_to_mrc.map(file_path=fps)
    mrc_results = convert_dm_mrc_to_jpeg.map(dm_to_mrc)

    mrc_tiff_results = convert_2d_mrc_to_tiff.map(
        file_path=fps, wait_for=[allow_failure(mrc_results)]
    )

    # Finally generate all valid suffixed results
    keyimg_assets = scale_jpegs.map(
        fps,
        size=unmapped("l"),
        wait_for=[allow_failure(tiff_results), allow_failure(mrc_tiff_results)],
    )
    thumb_assets = scale_jpegs.map(
        fps,
        size=unmapped("s"),
        wait_for=[allow_failure(tiff_results), allow_failure(mrc_tiff_results)],
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
