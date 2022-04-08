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
def make_work_dir(fname: Path) -> Path:
    """
    a temporary dir to house all files. Will be rm'd upon completion.
    dummy arg fname - using as placeholder - TODO
    """
    return Path(tempfile.mkdtemp(dir=Config.tmp_dir))


@task
def copy_template(working_dir: Path, template_name: str) -> Path:
    """
    copies the template adoc file to the working_dir,
    """
    adoc_fp = f"{working_dir}/{template_name}.adoc"
    shutil.copyfile(f"{Config.template_dir}/{template_name}.adoc", adoc_fp)
    return Path(adoc_fp)


@task
def list_input_dir(input_dir_fp: Path) -> List[Path]:
    """
    discover the contents of the input_dir AKA "Sample"
    note, only lists mrc files currently. TODO(?)
    include .st files TODO
    note, only processing first file ATM (test)
    """
    logger = prefect.context.get("logger")
    logger.info(f"trying to list {input_dir_fp}")
    mrc_files = glob.glob(f"{input_dir_fp}/*.mrc")
    # if len mrc_files == 0: raise?
    return Path(mrc_files)


@task
def prep_input_fp(fname: Path, working_dir: Path) -> Path:
    """
    copies tomographs (mrc files) into working_dir
    """
    fp = shutil.copyfile(src=fname.as_posix(), dst=f"{working_dir}/{fname.name}")
    return Path(fp)


@task
def update_adoc(
    adoc_fp: Path,
    tg_fp: Path,
    dual: str,
    montage: str,
    gold: str,
    focus: str,
    bfocus: str,
    fiducialless: str,
    trackingMethod: str,
    TwoSurfaces: str,
    TargetNumberOfBeads: str,
    LocalAlignments: str,
    THICKNESS: str,
) -> Path:
    """updates the adoc file with input params,"""
    file_loader = FileSystemLoader(str(adoc_fp.parent))
    env = Environment(loader=file_loader)
    template = env.get_template(adoc_fp.name)

    name = tg_fp.stem
    stackext = tg_fp.suffix
    currentBStackExt = "mrc"  # TODO - should be a tupple of fps
    datasetDirectory = adoc_fp.parent
    if TwoSurfaces == "0":
        SurfacesToAnalyze = 1
    elif TwoSurfaces == "1":
        SurfacesToAnalyze = 2
    else:
        raise signals.FAIL(
            f"Unable to resolve SurfacesToAnalyze, TwoSurfaces \
                is set to {TwoSurfaces}, and should be 0 or 1"
        )
    rpa_thickness = int(THICKNESS) * 1.5

    vals = {
        "name": name,
        "stackext": stackext,
        "currentBStackExt": currentBStackExt,
        "dual": dual,
        "montage": montage,
        "gold": gold,
        "focus": focus,
        "bfocus": bfocus,
        "datasetDirectory": datasetDirectory,
        "fiducialless": fiducialless,
        "trackingMethod": trackingMethod,
        "TwoSurfaces": TwoSurfaces,
        "TargetNumberOfBeads": TargetNumberOfBeads,
        "SurfacesToAnalyze": SurfacesToAnalyze,
        "LocalAlignments": LocalAlignments,
        "rpa_thickness": rpa_thickness,
        "THICKNESS": THICKNESS,
    }

    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{adoc_fp.stem}.adoc")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
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
def log(item: str) -> None:
    logger = prefect.context.get("logger")
    logger.info(item)


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
    mrc2nifti path/{basename}_full_rec.mrc path/{basename}.nii
    """
    return f"mrc2nifti {fp.parent}/{fp.stem}_full_rec.mrc {fp.parent}/{fp.stem}.nii"


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
    return f"mrc_visual_min_max {fp.parent}/{fp.stem}.nii --mad 5 --output-json {fp.parent}/mrc2ngpc-output.json"


if __name__ == "__main__":
    with Flow("brt_flow", executor=Config.SLURM_EXECUTOR) as f:
        # This block of params map are for adoc file specfication.
        # Note the ugly names, these parameters are lifted verbatim from
        # https://bio3d.colorado.edu/imod/doc/directives.html where possible.
        # (there are two thickness args, these are not verbatim.)
        dual = Parameter("dual")
        montage = Parameter("montage")
        gold = Parameter("gold")
        focus = Parameter("focus")
        bfocus = Parameter("bfocus")
        fiducialless = Parameter("fiducialless")
        trackingMethod = Parameter("trackingMethod")
        TwoSurfaces = Parameter("TwoSurfaces")
        TargetNumberOfBeads = Parameter("TargetNumberOfBeads")
        LocalAlignments = Parameter("LocalAlignments")
        THICKNESS = Parameter("THICKNESS")
        # end user facing adoc params

        adoc_template = Parameter("adoc_template")
        input_dir = Parameter("input_dir")
        callback_url = Parameter("callback_url")()
        token = Parameter("token")()
        sample_id = Parameter("sample_id")()
        # a single input_dir will have n tomograms
        input_dir_fp = utils.get_input_dir(input_dir=input_dir)
        fnames = list_input_dir(input_dir_fp=input_dir_fp)
        working_dirs = make_work_dir.map(fname=fnames)
        adoc_fps = copy_template.map(
            working_dir=working_dirs, template_name="plastic_brt"
        )
        updated_adocs = update_adoc.map(
            adoc_fp=adoc_fps,
            tg_fp=fnames,
            dual=dual,
            montage=montage,
            gold=gold,
            focus=focus,
            bfocus=bfocus,
            fiducialless=fiducialless,
            TwoSurfaces=TwoSurfaces,
            LocalAlignments=LocalAlignments,
            THICKNESS=THICKNESS,
        )
        log.map(item=updated_adocs)
        tomogram_fps = prep_input_fp.map(fname=fnames, working_dir=working_dirs)

        # START BRT (Batchruntomo) - long running process.
        brt_commands = create_brt_command.map(adoc_fp=updated_adocs)
        log.map(item=brt_commands)
        brts = shell_task.map(command=brt_commands, upstream_tasks=[tomogram_fps])
        brts_ok = check_brt_run_ok.map(tg_fp=tomogram_fps, upstream_tasks=[brts])
        # END BRT, check files for success (else fail here)

        # stack dimensions - used in movie creation
        xyz_dim_cmds = gen_dimension_command.map(
            tg_fp=tomogram_fps, upstream_tasks=[brts]
        )
        log.map(item=xyz_dim_cmds)
        xyz_dims = shell_task.map(command=xyz_dim_cmds)
        z_dims = split_dims(xyz_dims)
        # end dims

        # START TILT MOVIE GENERATION:
        newstack_cmds = gen_ns_cmnds.map(
            fp=tomogram_fps, z_dim=z_dims, upstream_tasks=[brts]
        )
        log.map(item=newstack_cmds)
        newstacks = shell_task.map(command=newstack_cmds)
        ns_float_cmds = gen_ns_float(fp=tomogram_fps, upstream_tasks=[newstacks])
        log.map(item=ns_float_cmds)
        ns_floats = shell_task.map(command=ns_float_cmds)
        mrc2tiff_cmds = gen_mrc2tiff.map(fp=tomogram_fps, upstream_tasks=[ns_floats])
        log.map(item=mrc2tiff_cmds)
        mrc2tiffs = shell_task.map(command=mrc2tiff_cmds)
        middle_is = calc_middle_i.map(z_dim=z_dims)
        gm_cmds = gen_gm_convert.map(
            fp=tomogram_fps, middle_i=middle_is, upstream_tasks=[mrc2tiffs]
        )
        log.map(item=gm_cmds)
        gms = shell_task.map(command=gm_cmds)
        mpeg_cmds = gen_ffmpeg_cmd.map(fp=tomogram_fps, upstream_tasks=[mrc2tiffs])
        log(mpeg_cmds)
        mpegs = shell_task.map(command=mpeg_cmds)
        # END TILT MOVIE GENERATION

        # START RECONSTR MOVIE GENERATION:
        clip_cmds = gen_clip_rc_cmds.map(
            fp=tomogram_fps, z_dim=z_dims, upstream_tasks=[brts]
        )
        log.map(clip_cmds)
        clips = shell_task.map(command=clip_cmds)
        ns_float_rc_cmds = newstack_fl_rc_cmd.map(
            fp=tomogram_fps, upstream_tasks=[clips]
        )
        log.map(item=ns_float_rc_cmds)
        ns_float_rcs = shell_task.map(command=ns_float_rc_cmds)
        bin_vol_cmds = gen_binvol_rc_cmd.map(
            fp=tomogram_fps, upstream_tasks=[ns_float_rcs]
        )
        bin_vols = shell_task.map(command=bin_vol_cmds)
        mrc2tiff_rc_cmds = gen_mrc2tiff_rc_cmd.map(
            fp=tomogram_fps, upstream_tasks=[bin_vols]
        )
        mrc2tiff_rcs = shell_task.map(command=mrc2tiff_rc_cmds)
        ffmpeg_rc_cmds = gen_ffmpeg_rc_cmd.map(
            fp=tomogram_fps, upstream_tasks=[mrc2tiff_rcs]
        )
        log.map(ffmpeg_rc_cmds)
        ffmpeg_rcs = shell_task.map(command=ffmpeg_rc_cmds)
        # END RECONSTR MOVIE

        # START PYRAMID GEN
        mrc2nifti_cmds = gen_mrc2nifti_cmd.map(fp=tomogram_fps, upstream_tasks=[brts])
        log.map(mrc2nifti_cmds)
        mrc2niftis = shell_task.map(command=mrc2nifti_cmds)
        pyramid_cmds = gen_pyramid_cmd.map(fp=tomogram_fps, upstream_tasks=[mrc2niftis])
        log.map(pyramid_cmds)
        gen_pyramids = shell_task.map(command=pyramid_cmds)
        min_max_cmds = gen_min_max_cmd.map(fp=tomogram_fps, upstream_tasks=[mrc2niftis])
        log.map(item=min_max_cmds)
        min_maxs = shell_task(command=min_max_cmds)
        # END PYRAMID
