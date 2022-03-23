#!/usr/bin/env python3
import re
import math
import glob

from typing import List

# import os
from pathlib import Path
import shutil
import tempfile
import prefect
from jinja2 import Environment, FileSystemLoader
from prefect import task, Flow, Parameter
from prefect.engine import signals
from prefect.tasks.shell import ShellTask


from image_portal_workflows.utils import utils
from image_portal_workflows import config
from image_portal_workflows.config import Config

shell_task = ShellTask()


@task
def make_work_dir() -> Path:
    """
    a temporary dir to house all files. Will be rm'd upon completion.
    """
    return Path(tempfile.mkdtemp(dir=Config.tmp_dir))


@task
def prep_adoc(working_dir: Path, fname: Path) -> Path:
    """
    copy the template adoc file to the working_dir,
    interpolate vars, TODO"""
    adoc_fp = f"{working_dir}/{fname.stem}.adoc_template"
    shutil.copyfile(f"{Config.template_dir}/dirTemplate.adoc", adoc_fp)
    return Path(adoc_fp)


@task
def list_input_dir(input_dir_fp: Path) -> Path:
    """
    discover the contents of the input_dir AKA "Sample"
    note, only lists mrc files currently. TODO(?)
    note, only processing first file ATM (test)
    """
    logger = prefect.context.get("logger")
    logger.info(f"trying to list {input_dir_fp}")
    mrc_files = glob.glob(f"{input_dir_fp}/*.mrc")
    # if len mrc_files == 0: raise?
    return Path(mrc_files[0])


@task
def prep_input_fp(fname: Path, working_dir: Path) -> Path:
    """
    copies tomographs (mrc files) into working_dir
    """
    fp = shutil.copyfile(src=fname.as_posix(), dst=f"{working_dir}/{fname.name}")
    return Path(fp)


@task
def update_adoc(adoc_fp: Path) -> Path:
    """updates the adoc file with input params, TODO"""
    file_loader = FileSystemLoader(str(adoc_fp.parent))
    env = Environment(loader=file_loader)
    template = env.get_template(adoc_fp.name)
    vals = {
        "basename": adoc_fp.stem,
        "bead_size": 10,
        # "pixel_size": 4,
        # "newstack_bin_by_fact": 4,
        "light_beads": 0,
        "tilt_thickness": 256,
        # "runtime_bin_by_fact": 4,
        "montage": 0,
        "dataset_dir": str(adoc_fp.parent),
    }

    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{adoc_fp.stem}.adoc")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
    logger = prefect.context.get("logger")
    logger.info(f"created {adoc_loc}")
    return adoc_loc


@task
def create_brt_command(adoc_fp: Path) -> str:
    cmd = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1"
    return cmd


@task
def gen_dimension_command(tg_fp: Path) -> str:
    command = f"header -s {tg_fp.parent}/{tg_fp.stem}_ali.mrc"
    return command


@task
def check_brt_run_ok(tg_fp: Path):
    """
    ensures the following files exist:
    BASENAME_rec.mrc - the source for the reconstruction movie
    BASENAME_full_rec.mrc - the source for the Neuroglancer pyramid
    BASENAME_ali.mrc
    """
    rec_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_rec.mrc")
    full_rec_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_full_rec.mrc")
    ali_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_ali.mrc")
    for _file in [rec_file, full_rec_file, ali_file]:
        if not _file.exists():
            raise signals.FAIL(f"File {_file} does not exist. BRT run failure.")


@task
def log(t):
    logger = prefect.context.get("logger")
    logger.info(t)


# @task
# def get_basename(fp: Path) -> str:
#    # basename = fp.stem[:-4]
#    return fp.stem


@task
def gen_ns_cmnds(fp: Path, z_dim) -> List[str]:
    cmds = list()
    for i in range(1, int(z_dim)):
        cmds.append(f"newstack -secs {i}-{i} {fp.as_posix()} {fp.parent}/{fp.stem}.mrc")
    return cmds


@task
def gen_ns_float(fp: Path) -> str:
    """
    newstack -float 3 path/BASENAME_ali*.mrc path/ali_BASENAME.mrc
    """
    in_fp = f"{fp.parent}/{fp.stem}*.mrc"
    out_fp = f"{fp.parent}/ali_{fp.stem}.mrc"
    return f"newstack -float 3 {in_fp} {out_fp}"


@task
def gen_mrc2tiff(fp: Path) -> str:
    """mrc2tif -j -C 0,255 path/ali_BASENAME.mrc path/BASENAME_ali"""
    in_fp = f"{fp.parent}/ali_{fp.stem}.mrc"
    out_fp = f"{fp.parent}/{fp.stem}_ali"
    return f"mrc2tif -j -C 0,255 {in_fp} {out_fp}"


@task
def gen_gm_convert(fp: Path, middle_i: str) -> str:
    """
    gm convert -size 300x300 path/BASENAME_ali.{MIDDLE_I}.jpg -resize 300x300 -sharpen 2 -quality 70 path/keyimg_BASENAME_s.jpg
    """
    middle_i_fname = f"{fp.parent}/{fp.stem}_ali.{middle_i}.jpg"
    key_img = f"{fp.parent}/keyimg_{fp.stem}_s.jpg"
    return f"gm convert -size 300x300 {middle_i_fname} -resize 300x300 -sharpen 2 -quality 70 {key_img}"


@task
def calc_middle_i(z_dim: str) -> str:
    """
    we want to find the middle image of the stack (for use as thumbnail)
    the file name later needed is padded with zeros
    TODO - this might be 3 or 4 - make this not a magic number.
    """
    fl = math.floor(int(z_dim) / 2)
    fl_padded = str(fl).rjust(3, "0")
    return fl_padded


@task
def gen_ffmpeg_cmd(fp: Path) -> str:
    """
    ffmpeg -f image2 -framerate 4 -i ${BASENAME}_ali.%03d.jpg -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 tiltMov_${BASENAME}.mp4
    """
    input_fp = f"{fp.parent}/{fp.stem}_ali.%03d.jpg"
    out_fp = f"{fp.parent}/tiltMov_{fp.stem}.mp4"
    return f"ffmpeg -y -f image2 -framerate 4 -i {input_fp} -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 {out_fp}"


@task
def split_dims(dims: List[int]):
    """
    dims comes in as a str of x,y,z dims
    just return third elt (first elt is empty) and assume it's Z
    """
    chunks = re.split(" +", str(dims))
    return chunks[3]


with Flow("brt_flow", executor=Config.SLURM_EXECUTOR) as flow:
    input_dir = Parameter("input_dir")
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    sample_id = Parameter("sample_id")()
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    fname = list_input_dir(input_dir_fp=input_dir_fp)
    working_dir = make_work_dir()
    adoc_fp = prep_adoc(working_dir, fname)
    updated_adoc = update_adoc(adoc_fp)
    brt_command = create_brt_command(adoc_fp=updated_adoc)
    log(brt_command)
    tomogram_fp = prep_input_fp(fname=fname, working_dir=working_dir)
    brt = shell_task(command=brt_command, upstream_tasks=[tomogram_fp])
    check_brt_run_ok(tg_fp=tomogram_fp, upstream_tasks=[brt])
    xyz_dim_cmd = gen_dimension_command(tg_fp=tomogram_fp, upstream_tasks=[brt])
    log(xyz_dim_cmd)
    xyz_dim = shell_task(command=xyz_dim_cmd)
    z_dim = split_dims(xyz_dim)
    newstack_cmds = gen_ns_cmnds(fp=tomogram_fp, z_dim=z_dim, upstream_tasks=[brt])
    newstacks = shell_task.map(command=newstack_cmds)
    ns_float_cmd = gen_ns_float(fp=tomogram_fp, upstream_tasks=[newstacks])
    log(ns_float_cmd)
    ns_float = shell_task(command=ns_float_cmd)
    mrc2tiff_cmd = gen_mrc2tiff(fp=tomogram_fp, upstream_tasks=[ns_float])
    log(mrc2tiff_cmd)
    mrc2tiff = shell_task(command=mrc2tiff_cmd)
    middle_i = calc_middle_i(z_dim)
    gm_cmd = gen_gm_convert(
        fp=tomogram_fp, middle_i=middle_i, upstream_tasks=[ns_float]
    )
    log(gm_cmd)
    gm = shell_task(command=gm_cmd)
    mpeg_cmd = gen_ffmpeg_cmd(fp=tomogram_fp, upstream_tasks=[ns_float])
    log(mpeg_cmd)
    mpeg = shell_task(command=mpeg_cmd)
