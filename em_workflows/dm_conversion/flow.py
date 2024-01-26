from pathlib import Path
from typing import Optional

from prefect import flow, task, allow_failure, unmapped
from pytools.meta import is_int16
from pytools.convert import file_to_uint8

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.constants import LARGE_DIM, AssetType
from em_workflows.dm_conversion.config import DMConfig
from em_workflows.dm_conversion.constants import LARGE_2D, SMALL_2D, VALID_2D_INPUT_EXTS


@task
def convert_dms_to_mrc(file_path: FilePath) -> None:
    dm_file_suffixes = [".dm3", ".dm4"]
    if file_path.fp_in.suffix.lower() in dm_file_suffixes:
        dm_as_mrc = file_path.gen_output_fp(out_fname="dm_as_mrc.mrc")
        msg = f"Using dir: {file_path.fp_in}, : creating dm_as_mrc {dm_as_mrc}"
        utils.log(msg=msg)
        log_file = f"{dm_as_mrc.parent}/dm2mrc.log"
        cmd = [DMConfig.dm2mrc_loc, file_path.fp_in.as_posix(), dm_as_mrc.as_posix()]
        # utils.log(f"Generated cmd {cmd}")
        FilePath.run(cmd=cmd, log_file=log_file)


@task
def convert_if_int16_tiff(file_path: FilePath) -> None:
    """
    accepts a tiff Path obj
    tests if 16 bit
    if 16 bit convert (write to assets_dir) & return Path
    else return orig Path
    """
    # first check if file extension is .tif or .tiff
    tif_suffixes = [".tiff", ".tif"]
    utils.log(f"Checkgin {file_path.fp_in.suffix} is a tiff file")
    if file_path.fp_in.suffix.lower() in tif_suffixes:
        utils.log(f"{file_path.fp_in.as_posix()} is a tiff file")
        if is_int16(file_path.fp_in):
            tif_8_bit = file_path.gen_output_fp(out_fname="as_8_bit.tif")
            utils.log(f"{file_path.fp_in} is a 16 bit tiff, converting to {tif_8_bit}")
            file_to_uint8(in_file_path=file_path.fp_in, out_file_path=str(tif_8_bit))


@task
def convert_2d_mrc_to_tiff(file_path: FilePath) -> None:
    """
    Checks Projects dir for mrc inputs. We assume anything in Projects will be 2D.
    Converts to tiff file, 1024 in size (using constant LARGE_DIM)

    env IMOD_OUTPUT_FORMAT=TIF newstack \
            --shrink $shrink_factor$ -antialias 6 -mode 0 -meansd 140,50 f_in.mrc f_out.tif
    where shrink_factor is input_size/1024
    used the smaller of x and y (4092/1024)
    """
    utils.log(f"Checking {file_path} for mrc inputs")
    if file_path.fp_in.suffix.lower() == ".mrc":
        # nifti_fp = neuroglancer.gen_niftis.__wrapped__(file_path)
        # utils.log(f"+++++++++++++++++++++++++++++++++++++++++++++")
        # min_max_histo = neuroglancer.gen_min_max_histo(file_path)
        # utils.log(min_max_histo)
        # utils.log(f"+++++++++++++++++++++++++++++++++++++++++++++")
        dims = utils.lookup_dims(file_path.fp_in)
        if dims.z != 1:
            msg = f"mrc file {file_path.fp_in} is not 2 dimensional. Contains {dims.z} Z dims."
            raise RuntimeError(msg)
        # use the min dimension of x & y to compute shrink_factor
        min_xy = min(dims.x, dims.y)
        # work out shrink_factor
        shrink_factor = min_xy / LARGE_DIM
        # round to 3 decimal places
        shrink_factor_3 = f"{shrink_factor:.3f}"
        out_fp = f"{file_path.working_dir}/{file_path.base}_mrc_as_tiff.tiff"
        log_fp = f"{file_path.working_dir}/{file_path.base}_mrc_as_tiff.log"
        utils.log(
            f"{file_path.fp_in.as_posix()} is a mrc file, will convert to {out_fp}."
        )
        # work out meansd
        cmd = [
            "env",
            "IMOD_OUTPUT_FORMAT=TIF",
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
        FilePath.run(cmd, log_fp)


@task
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


@task
def scale_jpegs(file_path: FilePath, size: str) -> Optional[dict]:
    """
    generates keyThumbnail and keyImage
    looks for file names <something>_mrc_as_jpg.jpeg
    """
    mrc_as_jpg = file_path.gen_output_fp(out_fname="mrc_as_jpg.jpeg")
    tif_8_bit = file_path.gen_output_fp(out_fname="as_8_bit.tif")
    mrc_as_tiff = Path(f"{file_path.working_dir}/{file_path.base}_mrc_as_tiff.tiff")
    tif_jpg_png_suffixes = [".tif", ".tiff", ".jpeg", ".png", ".jpg"]
    if mrc_as_jpg.exists():
        cur = mrc_as_jpg
    elif tif_8_bit.exists():
        cur = tif_8_bit
    elif mrc_as_tiff.exists():
        cur = mrc_as_tiff  # can ignore here if scaled
    elif file_path.fp_in.suffix.lower() in tif_jpg_png_suffixes:
        cur = file_path.fp_in
    else:
        msg = f"Impossible state for {file_path.fp_in}"
        raise RuntimeError(msg)
    if size.lower() == "s":
        output = file_path.gen_output_fp("_SM.jpeg")
        log = f"{output.parent}/jpeg_sm.log"
        asset_type = AssetType.THUMBNAIL
        cmd = [
            "gm",
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
            "gm",
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
    name="Flow: DM Conversion",
    log_prints=True,
    task_runner=DMConfig.SLURM_EXECUTOR,
    on_completion=[utils.notify_api_completion],
    on_failure=[utils.notify_api_completion],
)
def dm_flow(
    file_share: str,
    input_dir: str,
    file_name: str = None,
    callback_url: str = None,
    token: str = None,
    no_api: bool = False,
    keep_workdir: bool = False,
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
    input_dir_fp = utils.get_input_dir(share_name=file_share, input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        VALID_2D_INPUT_EXTS,
        single_file=file_name,
    )
    fps = utils.gen_fps(share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps)
    # logs = utils.init_log.map(file_path=fps)

    tiffs_converted = convert_if_int16_tiff.map(file_path=fps)

    # dm* to mrc conversion
    dm_to_mrc_converted = convert_dms_to_mrc.map(fps, wait_for=[tiffs_converted])

    # mrc is intermed format, to jpeg conversion
    mrc_to_jpeg = convert_dm_mrc_to_jpeg.map(fps, wait_for=[dm_to_mrc_converted])

    # mrc can also be an input, convert to tiff.
    convert_2d_mrc = convert_2d_mrc_to_tiff.map(file_path=fps)

    # scale the jpegs, pngs, and tifs
    keyimg_assets = scale_jpegs.map(
        fps, size=unmapped("l"), wait_for=[mrc_to_jpeg, convert_2d_mrc]
    )
    thumb_assets = scale_jpegs.map(
        fps, size=unmapped("s"), wait_for=[mrc_to_jpeg, convert_2d_mrc]
    )

    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_keyimgs = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=keyimg_assets
    )
    # finally filter error states, and convert to JSON and send.
    filtered_callback = utils.filter_results(callback_with_keyimgs)

    cb = utils.send_callback_body(
        no_api=no_api,
        token=token,
        callback_url=callback_url,
        files_elts=filtered_callback,
    )
    utils.cleanup_workdir(fps, keep_workdir, wait_for=[allow_failure(cb)])
