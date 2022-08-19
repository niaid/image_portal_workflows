from typing import Dict
import math
import shutil
import json
from pathlib import Path
import prefect
from prefect import task


@task
def gen_mrc2nifti_cmd(fp: Path) -> str:
    """
    mrc2nifti path/{basename}_full_rec.mrc path/{basename}.nii
    """
    rec_mrc_fp = Path(f"{fp.parent}/{fp.stem}_rec.mrc")
    base_mrc_fp = Path(f"{fp.parent}/{fp.stem}.mrc")
    if rec_mrc_fp.exists():
        mrc_fp = rec_mrc_fp
    elif base_mrc_fp.exists():
        mrc_fp = base_mrc_fp
    else:
        raise signals.FAIL(
            f"unable to find input for nifti generation. \
                {rec_mrc_fp} nor {base_mrc_fp}."
        )
    cmd = f"mrc2nifti {mrc_fp} {fp.parent}/{fp.stem}.nii"
    logger = prefect.context.get("logger")
    logger.info(f"mrc2nifti command: {cmd}")
    return cmd


@task
def gen_pyramid_outdir(fp: Path) -> Path:
    outdir = Path(f"{fp.parent}/neuro-{fp.stem}")
    if outdir.exists():
        shutil.rmtree(outdir)
        prefect.context.get("logger").info(
            f"Pyramid dir {outdir} already exists, overwriting."
        )
    return outdir


@task
def gen_pyramid_cmd(fp: Path, outdir: Path) -> str:
    """
    volume-to-precomputed-pyramid --downscaling-method=average --flat path/basename.nii path/neuro-basename
    """
    cmd = f"volume-to-precomputed-pyramid --downscaling-method=average \
            --flat {fp.parent}/{fp.stem}.nii {outdir}"
    prefect.context.get("logger").info(f"pyramid command: {cmd}")
    return cmd


@task
def gen_archive_pyramid_cmd(working_dir: Path, archive_name: Path) -> str:
    """
    cd working_dir && zip  --compression-method store  -r archive_name  ./* && cd -
    """
    cmd = f"cd {working_dir} && zip -q --compression-method store -r {archive_name} ./* && cd -"
    logger = prefect.context.get("logger")
    logger.info(f"gen_min_max_cmd command: {cmd}")
    return cmd


@task
def gen_min_max_cmd(fp: Path, out_fp: Path) -> str:
    """
    mrc_visual_min_max {basename}.nii --mad 5 --output-json mrc2ngpc-output.json
    """
    cmd = f"mrc_visual_min_max {fp.parent}/{fp.stem}.nii --mad 5 \
            --output-json {out_fp}"
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
        kv = json.load(_file)
    _floor = kv["neuroglancerPrecomputedFloor"]
    _limit = kv["neuroglancerPrecomputedLimit"]
    _min = kv["neuroglancerPrecomputedMin"]
    _max = kv["neuroglancerPrecomputedMax"]
#    _floor = kv.find("neuroglancerPrecomputedFloor")
#    _limit = kv.find("neuroglancerPrecomputedLimit")
#    _min = kv.find("neuroglancerPrecomputedMin")
#    _max = kv.find("neuroglancerPrecomputedMax")
    metadata = {
            "neuroglancerPrecomputedFloor": str(math.floor(_floor)),
            "neuroglancerPrecomputedMin": str(math.floor(_min)),
            "neuroglancerPrecomputedLimit": str(math.ceil(_limit)),
            "neuroglancerPrecomputedMax": str(math.ceil(_max)),
        }
    return metadata
