import os
from pathlib import Path
from typing import Optional
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
    # cur = file_path.current
    dm_file_suffixes = [".dm3", ".dm4"]
    if file_path.fp_in.suffix.lower() in dm_file_suffixes:
        dm_as_mrc = file_path.gen_output_fp(out_fname="dm_as_mrc.mrc")
        msg = f"Using dir: {file_path.fp_in}, : creating dm_as_mrc {dm_as_mrc}"
        utils.log(msg=msg)
        log_file = f"{dm_as_mrc.parent}/dm2mrc.log"
        cmd = [Config.dm2mrc_loc, file_path.fp_in.as_posix(), dm_as_mrc.as_posix()]
        # utils.log(f"Generated cmd {cmd}")
        FilePath.run(cmd=cmd, log_file=log_file)
        # file_path.update_current(dm_as_mrc)


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

    env IMOD_OUTPUT_FORMAT=TIF newstack \
            --shrink $shrink_factor$ -antialias 6 -mode 0 -meansd 140,50 f_in.mrc f_out.tif
    where shrink_factor is input_size/1024
    used the smaller of x and y (4092/1024)
    """
    utils.log(f"Checking {file_path} for mrc inputs")
    large_dim = 1024
    if file_path.fp_in.suffix.lower() == ".mrc":
        dims = utils.lookup_dims(file_path.fp_in)
        if dims.z != 1:
            msg = f"mrc file {file_path.fp_in} is not 2 dimensional. Contains {dims.z} Z dims."
            raise signals.FAIL(msg)
        # use the min dimension of x & y to compute shrink_factor
        min_xy = min(dims.x, dims.y)
        # work out shrink_factor
        shrink_factor = min_xy / large_dim
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
            Config.newstack_loc,
            "-shrink",
            shrink_factor_3,
            "-antialias",
            "4",
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
        cmd = [Config.mrc2tif_loc, "-j", dm_as_mrc.as_posix(), mrc_as_jpg.as_posix()]
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
        raise signals.FAIL(msg)
    if size.lower() == "s":
        output = file_path.gen_output_fp("_SM.jpeg")
        log = f"{output.parent}/jpeg_sm.log"
        asset_type = "thumbnail"
        cmd = [
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
            output.as_posix(),
        ]
    elif size.lower() == "l":
        output = file_path.gen_output_fp("_LG.jpeg")
        log = f"{output.parent}/jpeg_lg.log"
        asset_type = "keyImage"
        cmd = [
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
            output.as_posix(),
        ]
    else:
        msg = f"Jpeg scaler must have size argument set to 'l' or 's'"
        raise signals.FAIL(msg)
    FilePath.run(cmd, log)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=output)
    asset_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_fp)
    return asset_elt


#    assets_fp_lg = file_path.copy_to_assets_dir(fp_to_cp=output_lg)
#    prim_fp = file_path.prim_fp_elt
#    prim_fp = file_path.add_asset(
#        prim_fp=prim_fp, asset_fp=assets_fp_sm, asset_type="keyThumbnail"
#    )
#    prim_fp = file_path.add_asset(
#        prim_fp=prim_fp, asset_fp=assets_fp_lg, asset_type="keyImage"
#    )
#    return prim_fp


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


# @task
# def list_files(input_dir: Path, exts: List[str], single_file: str = None) -> List[Path]:
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
    -list all inputs (ie files of relevant input type)
    -create output dir for each.
    -convert all 16bit tiffs to 8 bit, write to Assets dir.
    -convert all dm3/dm4 files to mrc, write to Assets dir.
    -convert <name>dm_as_mrc.mrc files to jpegs.
    -convert all mrcs in Projects dir to jpegs.
    -convert all tiffs/pngs/jpegs to correct size for thumbs, "sm" and "lg"
    """
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url", default=None)()
    token = Parameter("token", default=None)()
    no_api = Parameter("no_api", default=None)()
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
    mrc_to_jpeg = convert_dm_mrc_to_jpeg.map(fps, upstream_tasks=[dm_to_mrc_converted])

    # mrc can also be an input, convert to tiff.
    convert_2d_mrc = convert_2d_mrc_to_tiff.map(file_path=fps)

    # scale the jpegs, pngs, and tifs
    keyimg_assets = scale_jpegs.map(
        fps, size=unmapped("l"), upstream_tasks=[mrc_to_jpeg, convert_2d_mrc]
    )
    thumb_assets = scale_jpegs.map(
        fps, size=unmapped("s"), upstream_tasks=[mrc_to_jpeg, convert_2d_mrc]
    )

    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_keyimgs = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=keyimg_assets
    )
    # finally filter error states, and convert to JSON and send.
    filtered_callback = utils.filter_results(callback_with_keyimgs)
    cp_wd_to_assets = utils.copy_workdirs.map(
        fps, upstream_tasks=[callback_with_keyimgs]
    )

    callback_sent = utils.send_callback_body(
        token=token,
        callback_url=callback_url,
        files_elts=filtered_callback,
    )
