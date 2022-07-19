from typing import Dict
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
    full_rec_mrc_fp = Path(f"{fp.parent}/{fp.stem}_full_rec.mrc")
    base_mrc_fp = Path(f"{fp.parent}/{fp.stem}.mrc")
    if full_rec_mrc_fp.exists():
        mrc_fp = full_rec_mrc_fp
    elif base_mrc_fp.exists():
        mrc_fp = base_mrc_fp
    else:
        raise signals.FAIL(
            f"unable to find input for nifti generation. \
                {full_rec_mrc_fp} nor {base_mrc_fp}."
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
    volume-to-precomputed-pyramid --downscaling-method=average --no-gzip --flat path/basename.nii path/neuro-basename
    """
    cmd = f"volume-to-precomputed-pyramid --downscaling-method=average --no-gzip \
            --flat {fp.parent}/{fp.stem}.nii {outdir}"
    prefect.context.get("logger").info(f"pyramid command: {cmd}")
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
    """
    with open(fp, "r") as _file:
        return json.load(_file)
