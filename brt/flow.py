#!/usr/bin/env python3
from os import environ
import subprocess
import re
import math
from typing import List

from pathlib import Path
import shutil
import prefect
from jinja2 import Environment, FileSystemLoader
from prefect import task, Flow, Parameter, unmapped, flatten, case
from prefect.tasks.control_flow import merge
from prefect.engine import signals

# from prefect.tasks.shell import ShellTask


from image_portal_workflows.utils import utils
from image_portal_workflows.utils import neuroglancer as ng
from image_portal_workflows.shell_task_echo import ShellTaskEcho
from image_portal_workflows.config import Config

shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


@task
def copy_template(working_dir: Path, template_name: str) -> Path:
    """
    copies the template adoc file to the working_dir,
    """
    adoc_fp = f"{working_dir}/{template_name}.adoc"
    template_fp = f"{Config.template_dir}/{template_name}.adoc"
    logger = prefect.context.get("logger")
    logger.info(f"trying to copy {template_fp} to {adoc_fp}")
    shutil.copyfile(template_fp, adoc_fp)
    return Path(adoc_fp)


@task
def copy_tg_to_working_dir(fname: Path, working_dir: Path) -> Path:
    """
    copies files (tomograms/mrc files) into working_dir
    returns Path of copied file
    """
    new_loc = Path(f"{working_dir}/{fname.name}")
    if fname.exists():
        shutil.copyfile(src=fname.as_posix(), dst=new_loc)
    else:
        fp_1 = Path(f"{fname.parent}/{fname.stem}a{fname.suffix}")
        fp_2 = Path(f"{fname.parent}/{fname.stem}b{fname.suffix}")
        if fp_1.exists() and fp_2.exists():
            shutil.copyfile(src=fp_1.as_posix(), dst=f"{working_dir}/{fp_1.name}")
            shutil.copyfile(src=fp_2.as_posix(), dst=f"{working_dir}/{fp_2.name}")
        else:
            raise signals.FAIL(f"Files missing. {fp_1},{fp_2}. BRT run failure.")
    return new_loc


@task
def update_adoc(
    adoc_fp: Path,
    tg_fp: Path,
    dual_p: bool,
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
    """
    Uses jinja templating to update the adoc file with input params.
    Some of these parameters are derived programatically.
    """
    file_loader = FileSystemLoader(str(adoc_fp.parent))
    env = Environment(loader=file_loader)
    template = env.get_template(adoc_fp.name)

    name = tg_fp.stem
    dual = 0
    currentBStackExt = None
    stackext = tg_fp.suffix[1:]
    if dual_p:
        dual = 1
        currentBStackExt = tg_fp.suffix[1:]  # TODO - assumes both files are same ext
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
    rpa_thickness = int(int(THICKNESS) * 1.5)

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

    # junk above for now.
    #    vals = {
    #        "basename": name,
    #        "bead_size": 10,
    #        "light_beads": 0,
    #        "tilt_thickness": 256,
    #        "montage": 0,
    #        "dataset_dir": str(adoc_fp.parent),
    #    }
    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{tg_fp.stem}.adoc")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
    prefect.context.get("logger").info(f"generated {adoc_loc}")
    return adoc_loc


@task
def gen_dimension_command(tg_fp: Path) -> str:
    command = f"header -s {tg_fp.parent}/{tg_fp.stem}_ali.mrc"
    a = subprocess.run(command, shell=True, check=True, capture_output=True)
    outputs = a.stdout
    xyz_dim = re.split(" +(\d+)", str(outputs))
    z_dim = xyz_dim[5]
    logger = prefect.context.get("logger")
    logger.info(f"z_dim: {z_dim:}")
    return z_dim


@task
def check_brt_run_ok(tg_fp: Path):
    """
    ensures the following files exist:
    BASENAME_rec.mrc - the source for the reconstruction movie
    BASENAME_full_rec.mrc - the source for the Neuroglancer pyramid
    BASENAME_ali.mrc
    """
    rec_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_rec.mrc")
    # full_rec_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_full_rec.mrc")
    ali_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_ali.mrc")
    prefect.context.get("logger").info(
        f"checking that dir {tg_fp.parent} contains ok BRT run"
    )

    for _file in [rec_file, ali_file]:
        if not _file.exists():
            raise signals.FAIL(f"File {_file} does not exist. BRT run failure.")


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
    logger = prefect.context.get("logger")
    logger.info(cmds)
    return cmds


@task
def gen_ns_float(fp: Path) -> str:
    """
    newstack -float 3 path/BASENAME_ali*.mrc path/ali_BASENAME.mrc
    """
    in_fp = f"{fp.parent}/{fp.stem}_ali*.mrc"
    out_fp = f"{fp.parent}/ali_{fp.stem}.mrc"
    cmd = f"newstack -float 3 {in_fp} {out_fp}"
    logger = prefect.context.get("logger")
    logger.info(cmd)
    return cmd


@task
def gen_mrc2tiff(fp: Path) -> str:
    """mrc2tif -j -C 0,255 path/ali_BASENAME.mrc path/BASENAME_ali"""
    in_fp = f"{fp.parent}/ali_{fp.stem}.mrc"
    out_fp = f"{fp.parent}/{fp.stem}_ali"
    cmd = f"mrc2tif -j -C 0,255 {in_fp} {out_fp}"
    logger = prefect.context.get("logger")
    logger.info(cmd)
    return cmd


@task
def gen_copy_keyimages(tomogram_fp: Path, middle_i: str, fp_out: Path) -> str:
    """
    generates the keyImage (by copying image i to keyImage.jpeg)
    fname_in and fname_out both derived from tomogram fp
    MIDDLE_I might always be an int
    cp path/BASENAME_ali.{MIDDLE_I}.jpg path/BASENAME_keyimg.jpg
    """
    fname_in = f"{tomogram_fp.parent}/{tomogram_fp.stem}_ali.{middle_i}.jpg"
    cmd = f"cp {fname_in} {fp_out}"
    prefect.context.get("logger").info(cmd)
    return cmd


@task
def gen_gm_convert(tomogram_fp: Path, middle_i: str, fp_out: Path) -> str:
    """
    generates the keyThumbnail
    fname_in and fname_out both derived from tomogram fp
    MIDDLE_I might always be an int
    gm convert -size 300x300 path/BASENAME_ali.{MIDDLE_I}.jpg -resize 300x300 -sharpen 2 -quality 70 path/keyimg_BASENAME_s.jpg
    """
    fname_in = f"{tomogram_fp.parent}/{tomogram_fp.stem}_ali.{middle_i}.jpg"
    cmd = f"gm convert -size 300x300 {fname_in} -resize 300x300 \
            -sharpen 2 -quality 70 {fp_out}"
    prefect.context.get("logger").info(cmd)
    return cmd


@task
def calc_middle_i(z_dim: str) -> str:
    """
    we want to find the middle image of the stack (for use as thumbnail)
    the file name later needed is padded with zeros
    TODO - this might be 3 or 4 - make this not a magic number.
    """
    fl = math.floor(int(z_dim) / 2)
    fl_padded = str(fl).rjust(3, "0")
    logger = prefect.context.get("logger")
    logger.info(f"middle i: {fl_padded}")
    return fl_padded


@task
def gen_ffmpeg_cmd(fp: Path, fp_out: Path) -> str:
    """
    generates the tilt moveie
    ffmpeg -f image2 -framerate 4 -i ${BASENAME}_ali.%03d.jpg -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 tiltMov_${BASENAME}.mp4
    """
    input_fp = f"{fp.parent}/{fp.stem}_ali.%03d.jpg"
    cmd = f"ffmpeg -y -f image2 -framerate 4 -i {input_fp} -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 {fp_out}"
    prefect.context.get("logger").info(f"ffmpeg command: {cmd}")
    return cmd


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
    logger = prefect.context.get("logger")
    logger.info(f"reconstruction clip_cmds : {cmds}")
    return cmds


@task
def newstack_fl_rc_cmd(fp: Path, fp_out: Path) -> str:
    """
    newstack -float 3 path/BASENAME_ave* path/ave_BASENAME.mrc
    """
    fp_in = f"{fp.parent}/{fp.stem}_ave*"
    cmd = f"sleep 10 && newstack -float 3 {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"reconstruction newstack_fl_rc_cmd {cmd}")
    return cmd


@task
def gen_binvol_rc_cmd(fp: Path, fp_out: Path) -> str:
    """
    binvol -binning 2 path/ave_BASENAME.mrc path/avebin8_BASENAME.mrc
    """
    fp_in = f"{fp.parent}/{fp.stem}_ave.mrc"
    cmd = f"binvol -binning 2 {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"reconstruction binvol command: {cmd}")
    return cmd


@task
def gen_mrc2tiff_rc_cmd(fp: Path) -> str:
    """
    mrc2tif -j -C 100,255 path/ave_BASNAME.mrc path/BASENAME_mp4
    """
    fp_in = f"{fp.parent}/{fp.stem}_ave.mrc"
    fp_out = f"{fp.parent}/{fp.stem}_mp4"
    cmd = f"mrc2tif -j -C 100,255 {fp_in} {fp_out}"
    logger = prefect.context.get("logger")
    logger.info(f"mrc2tiff command: {cmd}")
    return cmd


@task
def gen_ffmpeg_rc_cmd(fp: Path, fp_out: Path) -> str:
    """
    TODO - 3 or 4?
    ffmpeg -f image2 -framerate 8 -i path/BASENAME_mp4.%03d.jpg -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 path/keyMov_BASENAME.mp4
    """
    fp_in = f"{fp.parent}/{fp.stem}_mp4.%03d.jpg"
    cmd = f"ffmpeg -f image2 -framerate 8 -y -i {fp_in} -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 {fp_out}"
    logger = prefect.context.get("logger")
    logger.info(f"reconstruction ffmpeg command: {cmd}")
    return cmd


@task
def list_paired_files(fnames: List[Path]) -> List[Path]:
    """if the input is a paired/dual axis shot, we can trucate the a|b off
    the filename, and use that string from this point on.
    We need to ensure there are only paired inputs in this list.
    """
    maybes = list()
    pairs = list()
    # paired_fnames = [fname.stem for fname in fps]
    for fname in fnames:
        if fname.stem.endswith("a"):
            # remove the last char of fname (keep extension)
            fname_no_a = f"{fname.parent}/{fname.stem[:-1]}{fname.suffix}"
            maybes.append(fname_no_a)
    for fname in fnames:
        if fname.stem.endswith("b"):
            fname_no_b = f"{fname.parent}/{fname.stem[:-1]}{fname.suffix}"
            if fname_no_b in maybes:
                pairs.append(Path(fname_no_b))
    return pairs


@task
def check_inputs_paired(fps: List[Path]):
    """
    asks if there are ANY paired inputs in a dir.
    If there are, will return True, else False
    """
    fnames = [fname.stem for fname in fps]
    inputs_paired = False
    for fname in fnames:
        if fname.endswith("a"):
            # remove the last char, cat on a 'b' and lookup.
            pair_name = fname[:-1] + "b"
            if pair_name in fnames:
                inputs_paired = True
    prefect.context.get("logger").info(f"Are inputs paired? {inputs_paired}.")
    return inputs_paired


# if __name__ == "__main__":

with Flow(
    "brt_flow",
    executor=Config.SLURM_EXECUTOR,
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
) as f:

    # This block of params map are for adoc file specfication.
    # Note the ugly names, these parameters are lifted verbatim from
    # https://bio3d.colorado.edu/imod/doc/directives.html where possible.
    # (there are two thickness args, these are not verbatim.)
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
    # defines the environment to be used. Eg dev|qa|prod
    environment = Parameter("environment")()
    token = Parameter("token")()
    file_name = Parameter("file_name", default=None)
    # a single input_dir will have n tomograms
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    fnames = utils.list_files(
        input_dir=input_dir_fp, exts=["mrc", "st"], single_file=file_name
    )

    fnames_ok = utils.check_inputs_ok(fnames)

    # check if inputs are paired (only - bool)
    inputs_paired = check_inputs_paired(fnames)
    with case(inputs_paired, True):
        fnames_p = list_paired_files(fnames=fnames)
    with case(inputs_paired, False):
        fnames_np = fnames
    fnames_fin = merge(fnames_p, fnames_np)

    temp_dirs = utils.make_work_dir.map(
        fnames_fin, upstream_tasks=[unmapped(fnames_ok)]
    )
    adoc_fps = copy_template.map(
        working_dir=temp_dirs, template_name=unmapped(adoc_template)
    )
    updated_adocs = update_adoc.map(
        adoc_fp=adoc_fps,
        tg_fp=fnames_fin,
        dual_p=unmapped(inputs_paired),
        montage=unmapped(montage),
        gold=unmapped(gold),
        focus=unmapped(focus),
        bfocus=unmapped(bfocus),
        fiducialless=unmapped(fiducialless),
        trackingMethod=unmapped(trackingMethod),
        TwoSurfaces=unmapped(TwoSurfaces),
        TargetNumberOfBeads=unmapped(TargetNumberOfBeads),
        LocalAlignments=unmapped(LocalAlignments),
        THICKNESS=unmapped(THICKNESS),
    )
    tomogram_fps = copy_tg_to_working_dir.map(fname=fnames_fin, working_dir=temp_dirs)

    # START BRT (Batchruntomo) - long running process.
    brt_commands = utils.create_brt_command.map(adoc_fp=updated_adocs)
    brts = shell_task.map(
        command=brt_commands,
        upstream_tasks=[tomogram_fps],
        to_echo=unmapped("BRT commands"),
    )
    brts_ok = check_brt_run_ok.map(tg_fp=tomogram_fps, upstream_tasks=[brts])
    # END BRT, check files for success (else fail here)

    # stack dimensions - used in movie creation
    z_dims = gen_dimension_command.map(tg_fp=tomogram_fps, upstream_tasks=[brts_ok])

    # START TILT MOVIE GENERATION:
    newstack_cmds = gen_ns_cmnds.map(
        fp=tomogram_fps, z_dim=z_dims, upstream_tasks=[brts_ok]
    )
    newstacks = shell_task.map(
        command=flatten(newstack_cmds),
        upstream_tasks=[brts_ok],
        to_echo=unmapped("newstack tilt"),
    )
    ns_float_cmds = gen_ns_float.map(fp=tomogram_fps, upstream_tasks=[newstacks])
    ns_floats = shell_task.map(
        command=ns_float_cmds, to_echo=unmapped("ns_floats commands")
    )
    mrc2tiff_cmds = gen_mrc2tiff.map(fp=tomogram_fps, upstream_tasks=[ns_floats])
    mrc2tiffs = shell_task.map(
        command=mrc2tiff_cmds, to_echo=unmapped("mrc2tiff commands")
    )
    middle_is = calc_middle_i.map(z_dim=z_dims)

    # key_imgs are an output/asset - location needs to be reported.
    key_img_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps,
        upstream_tasks=[mrc2tiffs],
        output_ext=unmapped("_keyImg.jpeg"),
    )

    cp_keyimage_cmds = gen_copy_keyimages.map(
        tomogram_fp=tomogram_fps,
        middle_i=middle_is,
        fp_out=key_img_fps,
        upstream_tasks=[mrc2tiffs],
    )
    cp_keyImages = shell_task.map(
        command=cp_keyimage_cmds, to_echo=unmapped("copying keyImages")
    )

    thumbs_sm_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps,
        upstream_tasks=[mrc2tiffs],
        output_ext=unmapped("_SM.jpeg"),
    )
    gm_cmds_sm = gen_gm_convert.map(
        tomogram_fp=tomogram_fps,
        middle_i=middle_is,
        fp_out=thumbs_sm_fps,
        upstream_tasks=[mrc2tiffs],
    )
    gms_sm = shell_task.map(
        command=gm_cmds_sm, to_echo=unmapped("gm small thumbnail commands")
    )

    # START TILT MOVIE GENERATION - every tg => 1 movie
    tiltMovie_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps,
        upstream_tasks=[mrc2tiffs],
        output_ext=unmapped("_tiltMov.mp4"),
    )
    tiltMovie_cmds = gen_ffmpeg_cmd.map(
        fp=tomogram_fps, fp_out=tiltMovie_fps, upstream_tasks=[mrc2tiffs]
    )
    mpegs = shell_task.map(
        command=tiltMovie_cmds, to_echo=unmapped("tiltMovie comands")
    )
    # END TILT MOVIE GENERATION

    # START RECONSTR MOVIE GENERATION:
    # makes ave files:
    clip_cmds = gen_clip_rc_cmds.map(
        fp=tomogram_fps, z_dim=z_dims, upstream_tasks=[brts_ok]
    )
    clips = shell_task.map(
        command=flatten(clip_cmds), to_echo=unmapped("rc clip commands")
    )
    # # #
    ns_float_rc_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps, output_ext=unmapped("_ave.mrc")
    )
    ns_float_rc_cmds = newstack_fl_rc_cmd.map(
        fp=tomogram_fps, fp_out=ns_float_rc_fps, upstream_tasks=[clips]
    )
    # ns_float_rc_cmds = utils.to_command.map(ns_float_rc_cmds_and_ave_vol_fps)
    ns_float_rcs = shell_task.map(
        command=ns_float_rc_cmds, to_echo=unmapped("rc float commands")
    )
    # # #

    # # #
    avebin8_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps, output_ext=unmapped("_avebin8.mrc")
    )
    # bin_vol_cmds_and_avebin8_fps = gen_binvol_rc_cmd.map(fp=tomogram_fps, upstream_tasks=[ns_float_rcs])
    bin_vol_cmds = gen_binvol_rc_cmd.map(
        fp=tomogram_fps, fp_out=avebin8_fps, upstream_tasks=[ns_float_rcs]
    )
    bin_vols = shell_task.map(
        command=bin_vol_cmds, to_echo=unmapped("bin vols commands")
    )
    # # #
    mrc2tiff_rc_cmds = gen_mrc2tiff_rc_cmd.map(
        fp=tomogram_fps, upstream_tasks=[bin_vols]
    )
    mrc2tiff_rcs = shell_task.map(
        command=mrc2tiff_rc_cmds, to_echo=unmapped("mrc2tiff_rcs commands")
    )
    # # #
    mpegs_rc_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps, output_ext=unmapped("_keyMov.mp4")
    )
    ffmpeg_rc_cmds = gen_ffmpeg_rc_cmd.map(
        fp=tomogram_fps, fp_out=mpegs_rc_fps, upstream_tasks=[mrc2tiff_rcs]
    )
    ffmpeg_rcs = shell_task.map(
        command=ffmpeg_rc_cmds, to_echo=unmapped("ffmpeg_rc_cmds commands")
    )
    # END RECONSTR MOVIE

    # START PYRAMID GEN
    mrc2nifti_cmds = ng.gen_mrc2nifti_cmd.map(fp=tomogram_fps, upstream_tasks=[brts_ok])
    mrc2niftis = shell_task.map(command=mrc2nifti_cmds, to_echo=unmapped("mrc2nifti"))

    ##
    ng_fps = ng.gen_pyramid_outdir.map(fp=tomogram_fps)
    pyramid_cmds = ng.gen_pyramid_cmd.map(
        fp=tomogram_fps, outdir=ng_fps, upstream_tasks=[mrc2niftis]
    )
    gen_pyramids = shell_task.map(command=pyramid_cmds, to_echo=unmapped("gen pyramid"))
    ##

    ##
    min_max_fps = utils.gen_output_fp.map(
        input_fp=tomogram_fps, output_ext=unmapped("_min_max.json")
    )
    min_max_cmds = ng.gen_min_max_cmd.map(
        fp=tomogram_fps, out_fp=min_max_fps, upstream_tasks=[mrc2niftis]
    )
    min_maxs = shell_task.map(command=min_max_cmds, to_echo=unmapped("Min max"))
    metadatas = ng.parse_min_max_file.map(fp=min_max_fps, upstream_tasks=[min_maxs])
    # END PYRAMID

    # create assets directory
    assets_dir = utils.make_assets_dir(input_dir=input_dir_fp)

    # We generate a base element, and then tack
    # on asset elements. Every time we tack on an element we change the
    # base elemnt name. This awkward logic avoids Slurm / dask errors.

    # generate base element
    callback_base_elts = utils.gen_callback_elt.map(input_fname=tomogram_fps)

    # key images
    key_img_asset_fps = utils.copy_to_assets_dir.map(
        fp=key_img_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[cp_keyImages],
    )
    callback_with_keyImages = utils.add_assets_entry.map(
        base_elt=callback_base_elts,
        path=key_img_asset_fps,
        asset_type=unmapped("keyImage"),
    )

    # thumnails (small thumbs)
    thumbnail_fps = utils.copy_to_assets_dir.map(
        fp=thumbs_sm_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[gms_sm],
    )
    callback_with_thumbs = utils.add_assets_entry.map(
        base_elt=callback_with_keyImages,
        path=thumbnail_fps,
        asset_type=unmapped("thumbnail"),
    )

    # tiltMovie
    tiltMovie_asset_fps = utils.copy_to_assets_dir.map(
        fp=tiltMovie_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[mpegs],
    )
    callback_with_tiltMovie = utils.add_assets_entry.map(
        base_elt=callback_base_elts,
        path=tiltMovie_asset_fps,
        asset_type=unmapped("tiltMovie"),
    )

    # averagedVolume
    averagedVolume_asset_fps = utils.copy_to_assets_dir.map(
        fp=ns_float_rc_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[ns_float_rcs],
    )
    callback_with_ave_vol = utils.add_assets_entry.map(
        base_elt=callback_with_tiltMovie,
        path=averagedVolume_asset_fps,
        asset_type=unmapped("averagedVolume"),
    )

    # volume
    volume_asset_fps = utils.copy_to_assets_dir.map(
        fp=avebin8_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[bin_vols],
    )
    callback_with_bin_vols = utils.add_assets_entry.map(
        base_elt=callback_with_ave_vol,
        path=volume_asset_fps,
        asset_type=unmapped("volume"),
    )

    # recMovie
    recMovie_asset_fps = utils.copy_to_assets_dir.map(
        fp=mpegs_rc_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[ffmpeg_rcs],
    )
    callback_with_recMovie = utils.add_assets_entry.map(
        base_elt=callback_with_bin_vols,
        path=volume_asset_fps,
        asset_type=unmapped("recMovie"),
    )

    # neuroglancerPrecomputed
    ng_asset_fps = utils.copy_to_assets_dir.map(
        fp=ng_fps,
        assets_dir=unmapped(assets_dir),
        prim_fp=tomogram_fps,
        upstream_tasks=[gen_pyramids, metadatas],
    )
    callback_with_neuroglancer = utils.add_assets_entry.map(
        base_elt=callback_with_recMovie,
        path=ng_asset_fps,
        asset_type=unmapped("neuroglancerPrecomputed"),
        metadata=metadatas,
    )

    utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=callback_with_neuroglancer
    )

# print(json.dumps(result.result[files_elts]))
