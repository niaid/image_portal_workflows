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
    metadata = gen_metadata(fp_in=fp_in)
    ng_asset = fp_in.gen_asset(asset_type="neuroglancerPrecomputed", asset_fp=asset_fp)

    ng_asset["metadata"] = metadata
    return ng_asset

#  @task
#  def gen_pyramids(fp_in: FilePath) -> Dict:
#      """
#      converts nifti files into pyramid files.
#      volume-to-precomputed-pyramid --downscaling-method=average --flat path/basename.nii path/neuro-basename
#      """
#      # generate pyramid dir
#      outdir = fp_in.gen_output_fp(output_ext=f"/neuro-{fp_in.fp_in.stem}")
#      utils.log(f"Created pyramid outdir {outdir}")
#      nifti = fp_in.gen_output_fp(output_ext=".nii")
#      if outdir.exists():
#          shutil.rmtree(outdir)
#          prefect.context.get("logger").info(
#              f"Pyramid dir {outdir} already exists, overwriting."
#          )
#      log_file = f"{nifti.parent}/volume_to_precomputed_pyramid.log"
#      cmd = [
#          "volume-to-precomputed-pyramid",
#          "--downscaling-method=average",
#          "--flat",
#          "--no-gzip",
#          nifti.as_posix(),
#          outdir.as_posix(),
#      ]
#      utils.log(f"Created {cmd}")
#      FilePath.run(cmd=cmd, log_file=log_file)
#      asset_fp = fp_in.copy_to_assets_dir(fp_to_cp=outdir)
#      metadata = gen_metadata(fp_in=fp_in)
#      ng_asset = fp_in.gen_asset(asset_type="neuroglancerPrecomputed", asset_fp=asset_fp)
#      ng_asset["metadata"] = metadata
#      return ng_asset


def gen_metadata(fp_in: FilePath) -> Dict:
    min_max = fp_in.gen_output_fp(output_ext=f"_min_max.json")
    # first (/0) elt of image zarr array is used to gen stats
    zarr = fp_in.gen_output_fp(output_ext=".zarr/0")
    log_file = f"{zarr.parent}/mrc_visual_min_max.log"
    cmd = [
        "mrc_visual_min_max",
        zarr.as_posix(),
        "--mad",
        "5",
        "--output-json",
        min_max.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    with open(min_max, "r") as _file:
        metadata = json.load(_file)
    return metadata



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
#  def gen_pyramid_cmd(fp: Path, outdir: Path) -> str:
#      """
#      volume-to-precomputed-pyramid --downscaling-method=average --flat path/basename.nii path/neuro-basename
#      """
#      cmd = f"volume-to-precomputed-pyramid --downscaling-method=average --flat {fp.parent}/{fp.stem}.nii {outdir} &> {fp.parent}/volume_to_precomputed_pyramid.log"
#      prefect.context.get("logger").info(f"pyramid command: {cmd}")
#      return cmd


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


@task
def gen_min_max_cmd(fp: Path, out_fp: Path) -> str:
    """
    mrc_visual_min_max {basename}.nii --mad 5 --output-json mrc2ngpc-output.json
    mad == Median absolute deviation - determined empiricaly 5
    """
    cmd = f"mrc_visual_min_max {fp.parent}/{fp.stem}.nii --mad 5 --output-json {out_fp} &> {fp.parent}/mrc_visual_min_max.log"
    logger = prefect.context.get("logger")
    logger.info(f"gen_min_max_cmd command: {cmd}")
    return cmd


@task
def parse_min_max_file(fp: Path) -> Dict[str, str]:
    """
    min max command is run as a subprocess and dumped to a file.
    This file needs to be parsed.
    Should be four keys:
        neuroglancerPrecomputedMin,
        neuroglancerPrecomputedMax,
        neuroglancerPrecomputedFloor,
        neuroglancerPrecomputedLimit
    Values should be ints.
    Round min and floor: down, round max and limit: up.
    """
    with open(fp, "r") as _file:
        metadata = json.load(_file)
        # kv = json.load(_file)
    #    _floor = kv["neuroglancerPrecomputedFloor"]
    #    _limit = kv["neuroglancerPrecomputedLimit"]
    #    _min = kv["neuroglancerPrecomputedMin"]
    #    _max = kv["neuroglancerPrecomputedMax"]
    #    _floor = kv.find("neuroglancerPrecomputedFloor")
    #    _limit = kv.find("neuroglancerPrecomputedLimit")
    #    _min = kv.find("neuroglancerPrecomputedMin")
    #    _max = kv.find("neuroglancerPrecomputedMax")
    #    metadata = {
    #            "neuroglancerPrecomputedFloor": str(math.floor(_floor)),
    #            "neuroglancerPrecomputedMin": str(math.floor(_min)),
    #            "neuroglancerPrecomputedLimit": str(math.ceil(_limit)),
    #            "neuroglancerPrecomputedMax": str(math.ceil(_max)),
    #        }
    return metadata
