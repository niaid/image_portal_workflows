from em_workflows.config import Config
import subprocess
from em_workflows.file_path import FilePath
from em_workflows.utils import utils
from typing import Dict
import math
import shutil
import json
from pathlib import Path
import prefect
from prefect import task
from pytools.workflow_functions import visual_min_max
import pytools
import logging
import sys

@task
def gen_zarr(fp_in: FilePath, width: int, height: int, depth: int=None) -> Dict:

    """
    bioformats2raw --scale-format-string '%2$d' --downsample-type AREA --compression=blosc --compression-properties cname=zstd --compression-properties clevel=5 --compression-properties shuffle=1 --tile_width 512 --tile_height 512  input.mrc output.zarr

    bioformats2raw --scale-format-string '%2$d' --compression=blosc --compression-properties cname=zstd --compression-properties clevel=5 --compression-properties shuffle=1  --resolutions 1 --chunk_depth 128 --tile_width 128 --tile_height 128 input.mrc output.zarr
    uses bioformats
    """
    zarr = fp_in.gen_output_fp(output_ext=".zarr")
    rec_mrc = fp_in.gen_output_fp(output_ext="_rec.mrc")
    base_mrc = fp_in.gen_output_fp(output_ext=".mrc", out_fname="adjusted.mrc")
    input_mrc = fp_in.fp_in

    utils.log(f"Created zarr ")
    if rec_mrc.is_file():
        input_file = rec_mrc.as_posix()
    elif base_mrc.is_file():
        input_file = base_mrc.as_posix()
    elif input_mrc.is_file():
        input_file = input_mrc.as_posix()
    else:
        raise ValueError(f"unable to find input for zarr generation.")

    cmd = [
        Config.bioformats2raw,
        "--scale-format-string",
        "%2$d",
        "--compression",
        "blosc",
        "--compression-properties",
        "cname=zstd",
        "--compression-properties",
        "clevel=5",
        "--compression-properties",
        "shuffle=1",
        "--resolutions",
        "1",
        "--tile_width",
        str(width),
        "--tile_height",
        str(height)
    ]
    if depth:
        cmd.extend(["--chunk_depth", str(depth)])
    else:
        # if there's no depth we know it's a 2dmrc input
        cmd.extend(['--downsample-type', 'AREA'])

    cmd.extend([input_file, zarr.as_posix()])

    log_file = f"{zarr.parent}/mrc2zarr.log"
    utils.log(f"Created zarr {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    utils.log(f"biulding multiscales")
    cmd_ms = ["zarr_build_multiscales", zarr.as_posix()]
    FilePath.run(cmd=cmd_ms, log_file=log_file)
    asset_fp = fp_in.copy_to_assets_dir(fp_to_cp=zarr)
    first_zarr_arr = Path(zarr.as_posix() + "/0")
    pytools.logger.level = logging.DEBUG
    BASIC_FORMAT = "%(levelname)s:%(name)s:%(message)s"
    formatter = logging.Formatter(BASIC_FORMAT)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    pytools.logger.addHandler(handler)

    metadata = visual_min_max(mad_scale=5, input_image=first_zarr_arr)
    ng_asset = fp_in.gen_asset(asset_type="neuroglancerPrecomputed", asset_fp=asset_fp)

    ng_asset["metadata"] = metadata
    return ng_asset


#  @task
#  def gen_pyramid_outdir(fp: Path) -> Path:
#      outdir = Path(f"{fp.parent}/neuro-{fp.stem}")
#      if outdir.exists():
#          shutil.rmtree(outdir)
#          prefect.context.get("logger").info(
#              f"Pyramid dir {outdir} already exists, overwriting."
#          )
#      return outdir




#  @task
#  def gen_archive_pyr(file_path: FilePath) -> None:
#      """
#      zip  --compression-method store  -r archive_name  ./* && cd -
#      """
#      ng_dir = f"{file_path.working_dir}/neuro-{file_path.fp_in.stem}"
#      archive_name = f"{file_path.base}.zip"
#
#      cmd = ["zip", "-q", "--compression-method", "store", "-r", archive_name, ng_dir]
#      logger = prefect.context.get("logger")
#      logger.info(f"compressing : {cmd}")
#      subprocess.run(cmd, cwd=file_path.working_dir)

