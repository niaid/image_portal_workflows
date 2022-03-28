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
from image_portal_workflows.config import Config

shell_task = ShellTask(log_stderr=True)


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
    """
    newstack -secs {i}-{i} path/BASENAME_ali*.mrc WORKDIR/hedwig/BASENAME_ali{i}.mrc
    """
    cmds = list()
    for i in range(1, int(z_dim)):
        cmds.append(
            f"newstack -secs {i}-{i} {fp.parent}/{fp.stem}_ali*.mrc {fp.parent}/{fp.stem}.mrc"
        )
    return cmds


@task
def gen_ns_float(fp: Path) -> str:
    """
    newstack -float 3 path/BASENAME_ali*.mrc path/ali_BASENAME.mrc
    """
    in_fp = f"{fp.parent}/{fp.stem}_ali*.mrc"
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
    TODO typing here is wrong
    dims comes in as a str of x,y,z dims
    just return third elt (first elt is empty) and assume it's Z
    """
    chunks = re.split(" +", str(dims))
    return chunks[3]


# "_rc" denotes ReConstruction movie related funcs
@task
def gen_clip_rc_cmds(fp: Path, z_dim) -> List[str]:
    """
    for i in range(2, dimensions.z-2):
       IZMIN = i-2
       IZMAX = i+2
       clip avg -2d -iz IZMIN-IZMAX  -m 1 path/BASENAME_rec.mrc path/hedwig/BASENAME_ave${I}.mrc
    """
    cmds = list()
    for i in range(2, int(z_dim) - 2):
        izmin = i - 2
        izmax = i + 2
        in_fp = f"{fp.parent}/{fp.stem}_rec.mrc"
        out_fp = f"{fp.parent}/{fp.stem}_ave{str(i)}.mrc"
        cmd = f"clip avg -2d -iz {str(izmin)}-{str(izmax)} -m 1 {in_fp} {out_fp}"
        cmds.append(cmd)
    return cmds


@task
def newstack_fl_rc_cmd(fp: Path) -> str:
    """
    newstack -float 3 path/BASENAME_ave* path/ave_BASENAME.mrc
    """
    fp_in = f"{fp.parent}/{fp.stem}_ave*"
    fp_out = f"{fp.parent}/ave_{fp.stem}.mrc"
    return f"newstack -float 3 {fp_in} {fp_out}"


@task
def gen_binvol_rc_cmd(fp: Path) -> str:
    """
    binvol -binning 2 path/ave_BASENAME.mrc path/avebin8_BASENAME.mrc
    """
    fp_in = f"{fp.parent}/ave_{fp.stem}.mrc"
    fp_out = f"{fp.parent}/avebin8_{fp.stem}.mrc"
    return f"binvol -binning 2 {fp_in} {fp_out}"


@task
def gen_mrc2tiff_rc_cmd(fp: Path) -> str:
    """
    mrc2tif -j -C 100,255 path/ave_BASNAME.mrc path/BASENAME_mp4
    """
    fp_in = f"{fp.parent}/ave_{fp.stem}.mrc"
    fp_out = f"{fp.parent}/{fp.stem}_mp4"
    return f"mrc2tif -j -C 100,255 {fp_in} {fp_out}"


@task
def gen_ffmpeg_rc_cmd(fp: Path) -> str:
    """
    TODO - 3 or 4?
    ffmpeg -f image2 -framerate 8 -i path/BASENAME_mp4.%03d.jpg -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 path/keyMov_BASENAME.mp4
    """
    fp_in = f"{fp.parent}/{fp.stem}_mp4.%03d.jpg"
    fp_out = f"{fp.parent}/keyMov_{fp.stem}.mp4"
    return f"ffmpeg -f image2 -framerate 8 -i {fp_in} -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 {fp_out}"


@task
def gen_mrc2nifti_cmd(fp: Path) -> str:
    """
    mrc2nifti path/{basename}.mrc path/{basename}.nii
    """
    return f"mrc2nifti {fp.parent}/{fp.stem}.mrc {fp.parent}/{fp.stem}.nii"
    
@task
def gen_pyramid_cmd(fp: Path) -> str:
    """
    volume-to-precomputed-pyramid --downscaling-method=average --no-gzip --flat path/basename.nii path/neuro-basename
    """
    return f"volume-to-precomputed-pyramid --downscaling-method=average --no-gzip --flat {fp.parent}/{fp.stem}.nii {fp.parent}/neuro-{fp.stem}"

@task
def gen_min_max_cmd(fp: Path) -> str:
    """
    mrc_visual_min_max {basename}.nii --mad 5 --output-json mrc2ngpc-output.json
    """ 
    return f"mrc_visual_min_max {fp.parent}/{fp.stem}.nii --mad 5 --output-json mrc2ngpc-output.json"


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
    tomogram_fp = prep_input_fp(fname=fname, working_dir=working_dir)

    # START BRT (Batchruntomo) - long running process.
    brt_command = create_brt_command(adoc_fp=updated_adoc)
    log(brt_command)
    brt = shell_task(command=brt_command, upstream_tasks=[tomogram_fp])
    check_brt_run_ok(tg_fp=tomogram_fp, upstream_tasks=[brt])
    # END BRT, check files for success (else fail here)

    # stack dimensions - used in movie creation
    xyz_dim_cmd = gen_dimension_command(tg_fp=tomogram_fp, upstream_tasks=[brt])
    log(xyz_dim_cmd)
    xyz_dim = shell_task(command=xyz_dim_cmd)
    z_dim = split_dims(xyz_dim)
    # end dims

    # START TILT MOVIE GENERATION:
    newstack_cmds = gen_ns_cmnds(fp=tomogram_fp, z_dim=z_dim, upstream_tasks=[brt])
    log(newstack_cmds)
    newstacks = shell_task.map(command=newstack_cmds)
    ns_float_cmd = gen_ns_float(fp=tomogram_fp, upstream_tasks=[newstacks])
    log(ns_float_cmd)
    ns_float = shell_task(command=ns_float_cmd)
    mrc2tiff_cmd = gen_mrc2tiff(fp=tomogram_fp, upstream_tasks=[ns_float])
    log(mrc2tiff_cmd)
    mrc2tiff = shell_task(command=mrc2tiff_cmd)
    middle_i = calc_middle_i(z_dim)
    gm_cmd = gen_gm_convert(
        fp=tomogram_fp, middle_i=middle_i, upstream_tasks=[mrc2tiff]
    )
    log(gm_cmd)
    gm = shell_task(command=gm_cmd)
    mpeg_cmd = gen_ffmpeg_cmd(fp=tomogram_fp, upstream_tasks=[mrc2tiff])
    log(mpeg_cmd)
    mpeg = shell_task(command=mpeg_cmd)
    # END TILT MOVIE GENERATION

    # START RECONSTR MOVIE GENERATION:
    clip_cmds = gen_clip_rc_cmds(fp=tomogram_fp, z_dim=z_dim, upstream_tasks=[brt])
    log.map(clip_cmds)
    clips = shell_task.map(command=clip_cmds)
    ns_float_rc_cmd = newstack_fl_rc_cmd(fp=tomogram_fp, upstream_tasks=[clips])
    log(ns_float_rc_cmd)
    ns_float_rc = shell_task(command=ns_float_rc_cmd)
    bin_vol_cmd = gen_binvol_rc_cmd(fp=tomogram_fp, upstream_tasks=[ns_float_rc])
    bin_vol = shell_task(command=bin_vol_cmd)
    mrc2tiff_rc_cmd = gen_mrc2tiff_rc_cmd(fp=tomogram_fp, upstream_tasks=[bin_vol])
    mrc2tiff_rc = shell_task(command=mrc2tiff_rc_cmd)
    ffmpeg_rc_cmd = gen_ffmpeg_rc_cmd(fp=tomogram_fp, upstream_tasks=[mrc2tiff_rc])
    log(ffmpeg_rc_cmd)
    ffmpeg_rc = shell_task(command=ffmpeg_rc_cmd)
    # END RECONSTR MOVIE

    # START PYRAMID GEN
    mrc2nifti_cmd = gen_mrc2nifti_cmd(fp=tomogram_fp)
    log(mrc2nifti_cmd)
    mrc2nifti = shell_task(command=mrc2nifti_cmd)
    pyramid_cmd = gen_pyramid_cmd(fp=tomogram_fp, upstream_tasks=[mrc2nifti])
    log(pyramid_cmd)
    gen_pyramid = shell_task(command=pyramid_cmd)
    min_max_cmd = gen_min_max_cmd(fp=tomogram_fp, upstream_tasks=[mrc2nifti])
    log(min_max_cmd)
    min_max = shell_task(command=min_max_cmd)
    # END PYRAMID

