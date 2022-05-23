#!/usr/bin/env python3
import json
import subprocess
import re
import math
import glob
from typing import List, Dict

from pathlib import Path
import shutil
import tempfile
import prefect
from jinja2 import Environment, FileSystemLoader
from prefect import task, Flow, Parameter, unmapped, flatten
from prefect.engine import signals

# from prefect.tasks.shell import ShellTask


from image_portal_workflows.utils import utils
from image_portal_workflows.shell_task_echo import ShellTaskEcho
from image_portal_workflows.config import Config

shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


@task
def make_work_dir(fname) -> Path:
    """
    a temporary dir to house all files in the form:
    {Config.tmp_dir}{fname.stem}.
    eg: /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/tomogram_fname/
    Will be rm'd upon completion.
    """
    working_dir = Path(tempfile.mkdtemp(dir=f"{Config.tmp_dir}"))
    prefect.context.get("logger").info(
        f"created working_dir {working_dir} for {fname.as_posix()}"
    )
    return Path(working_dir)


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
    if len(mrc_files) == 0:
        raise signals.FAIL(f"Unable to find any input files in dir: {input_dir_fp}")
    mrc_fps = [Path(f) for f in mrc_files]
    # TESTING IGNORE
    mrc_fps = [Path(f"/home/macmenaminpe/data/brt_run/Projects/ABC2/2013-1220-dA30_5-BSC-1_10.mrc")]
    logger = prefect.context.get("logger")
    logger.info(f"Found {mrc_fps}")
    return mrc_fps


@task
def copy_tg_to_working_dir(fname: Path, working_dir: Path) -> Path:
    """
    copies files (tomograms/mrc files) into working_dir
    returns Path of copied file
    """
    # TESTING IGNORE
    fp = Path(f"/home/macmenaminpe/data/brt_run/Projects/ABC2/{fname.name}")
    # fp = shutil.copyfile(src=fname.as_posix(), dst=f"{working_dir}/{fname.name}")
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

    # junk above for now.
    vals = {
        "basename": name,
        "bead_size": 10,
        "light_beads": 0,
        "tilt_thickness": 256,
        "montage": 0,
        "dataset_dir": str(adoc_fp.parent),
    }
    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{tg_fp.stem}.adoc")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
    logger = prefect.context.get("logger")
    logger.info(f"generated {adoc_loc}")
    return adoc_loc


@task
def create_brt_command(adoc_fp: Path) -> str:
    cmd = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1"
    logger = prefect.context.get("logger")
    logger.info(cmd)
    #return cmd
    # to short test
    return "ls"


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
    full_rec_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_full_rec.mrc")
    ali_file = Path(f"{tg_fp.parent}/{tg_fp.stem}_ali.mrc")
    for _file in [rec_file, full_rec_file, ali_file]:
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
def gen_gm_convert(fp: Path, middle_i: str) -> List[str]:
    """
    generates the keyThumbnail
    gm convert -size 300x300 path/BASENAME_ali.{MIDDLE_I}.jpg -resize 300x300 -sharpen 2 -quality 70 path/keyimg_BASENAME_s.jpg
    """
    middle_i_fname = f"{fp.parent}/{fp.stem}_ali.{middle_i}.jpg"
    thumbnail_fp = f"{fp.parent}/keyimg_{fp.stem}_s.jpg"
    cmd = f"gm convert -size 300x300 {middle_i_fname} -resize 300x300 \
            -sharpen 2 -quality 70 {thumbnail_fp}"
    logger = prefect.context.get("logger")
    logger.info(cmd)
    return [cmd, thumbnail_fp]


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
def gen_ffmpeg_cmd(fp: Path) -> List[str]:
    """
    generates the tilt moveie
    ffmpeg -f image2 -framerate 4 -i ${BASENAME}_ali.%03d.jpg -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 tiltMov_${BASENAME}.mp4
    """
    input_fp = f"{fp.parent}/{fp.stem}_ali.%03d.jpg"
    out_fp = f"{fp.parent}/tiltMov_{fp.stem}.mp4"
    cmd = f"ffmpeg -y -f image2 -framerate 4 -i {input_fp} -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 {out_fp}"
    logger = prefect.context.get("logger")
    logger.info(f"ffmpeg command: {cmd}")
    return [cmd, out_fp]


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
def newstack_fl_rc_cmd(fp: Path) -> List[str]:
    """
    newstack -float 3 path/BASENAME_ave* path/ave_BASENAME.mrc
    """
    fp_in = f"{fp.parent}/{fp.stem}_ave*"
    fp_out = f"{fp.parent}/ave_{fp.stem}.mrc"
    cmd = f"sleep 10 && newstack -float 3 {fp_in} {fp_out}"
    logger = prefect.context.get("logger")
    logger.info(f"reconstruction newstack_fl_rc_cmd {cmd}")
    return [cmd, fp_out]


@task
def gen_binvol_rc_cmd(fp: Path) -> List[str]:
    """
    binvol -binning 2 path/ave_BASENAME.mrc path/avebin8_BASENAME.mrc
    """
    fp_in = f"{fp.parent}/ave_{fp.stem}.mrc"
    fp_out = f"{fp.parent}/avebin8_{fp.stem}.mrc"
    cmd = f"binvol -binning 2 {fp_in} {fp_out}"
    logger = prefect.context.get("logger")
    logger.info(f"reconstruction binvol command: {cmd}")
    return [cmd, fp_out]


@task
def gen_mrc2tiff_rc_cmd(fp: Path) -> str:
    """
    mrc2tif -j -C 100,255 path/ave_BASNAME.mrc path/BASENAME_mp4
    """
    fp_in = f"{fp.parent}/ave_{fp.stem}.mrc"
    fp_out = f"{fp.parent}/{fp.stem}_mp4"
    cmd = f"mrc2tif -j -C 100,255 {fp_in} {fp_out}"
    logger = prefect.context.get("logger")
    logger.info(f"mrc2tiff command: {cmd}")
    return cmd


@task
def gen_ffmpeg_rc_cmd(fp: Path) -> List[str]:
    """
    TODO - 3 or 4?
    ffmpeg -f image2 -framerate 8 -i path/BASENAME_mp4.%03d.jpg -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 path/keyMov_BASENAME.mp4
    """
    fp_in = f"{fp.parent}/{fp.stem}_mp4.%03d.jpg"
    fp_out = f"{fp.parent}/keyMov_{fp.stem}.mp4"
    cmd = f"ffmpeg -f image2 -framerate 8 -y -i {fp_in} -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 {fp_out}"
    logger = prefect.context.get("logger")
    logger.info(f"reconstruction ffmpeg command: {cmd}")
    return [cmd, fp_out]


@task
def gen_mrc2nifti_cmd(fp: Path) -> str:
    """
    mrc2nifti path/{basename}_full_rec.mrc path/{basename}.nii
    """
    cmd = f"mrc2nifti {fp.parent}/{fp.stem}_full_rec.mrc {fp.parent}/{fp.stem}.nii"
    logger = prefect.context.get("logger")
    logger.info(f"mrc2nifti command: {cmd}")
    return cmd


@task
def gen_pyramid_cmd(fp: Path) -> List[str]:
    """
    volume-to-precomputed-pyramid --downscaling-method=average --no-gzip --flat path/basename.nii path/neuro-basename
    """
    outdir = Path(f"{fp.parent}/neuro-{fp.stem}")
    logger = prefect.context.get("logger")
    if outdir.exists():
        shutil.rmtree(outdir)
        logger.info(f"removing {outdir}")
    cmd = f"volume-to-precomputed-pyramid --downscaling-method=average --no-gzip \
            --flat {fp.parent}/{fp.stem}.nii {outdir}"
    logger = prefect.context.get("logger")
    logger.info(f"pyramid command: {cmd}")
    return [cmd, outdir.as_posix()]


@task
def gen_min_max_cmd(fp: Path) -> List[str]:
    """
    mrc_visual_min_max {basename}.nii --mad 5 --output-json mrc2ngpc-output.json
    """
    out_fp = f"{fp.parent}/mrc2ngpc-output.json"
    cmd = f"mrc_visual_min_max {fp.parent}/{fp.stem}.nii --mad 5 \
            --output-json {out_fp}"
    logger = prefect.context.get("logger")
    logger.info(f"gen_min_max_cmd command: {cmd}")
    return [cmd, out_fp]


@task
def parse_min_max_file(l: List) -> Dict[str, str]:
    fp = l[1]
    with open(fp, "r") as _file:
        return json.load(_file)


# with Flow("brt_flow", executor=Config.SLURM_EXECUTOR) as f:
with Flow("brt_flow") as f:
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
    temp_dirs = make_work_dir.map(fnames)
    adoc_fps = copy_template.map(
        working_dir=temp_dirs, template_name=unmapped(adoc_template)
    )
    updated_adocs = update_adoc.map(
        adoc_fp=adoc_fps,
        tg_fp=fnames,
        dual=unmapped(dual),
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
    tomogram_fps = copy_tg_to_working_dir.map(fname=fnames, working_dir=temp_dirs)

    # START BRT (Batchruntomo) - long running process.
    brt_commands = create_brt_command.map(adoc_fp=updated_adocs)
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
    gm_cmds_and_thumbnail_locs = gen_gm_convert.map(
        fp=tomogram_fps, middle_i=middle_is, upstream_tasks=[mrc2tiffs]
    )
    gm_cmds = utils.to_command.map(cmd_and_fp=gm_cmds_and_thumbnail_locs)
    gms = shell_task.map(command=gm_cmds, to_echo=unmapped("gm thumbnail commands"))

    mpeg_cmds_and_tiltMovie_fps = gen_ffmpeg_cmd.map(
        fp=tomogram_fps, upstream_tasks=[mrc2tiffs]
    )
    mpeg_cmds = utils.to_command.map(mpeg_cmds_and_tiltMovie_fps)
    mpegs = shell_task.map(command=mpeg_cmds, to_echo=unmapped("ffmpeg comands"))
    # END TILT MOVIE GENERATION

    # START RECONSTR MOVIE GENERATION:
    # makes ave files:
    clip_cmds = gen_clip_rc_cmds.map(
        fp=tomogram_fps, z_dim=z_dims, upstream_tasks=[brts_ok]
    )
    clips = shell_task.map(
        command=flatten(clip_cmds), to_echo=unmapped("rc clip commands")
    )
    ns_float_rc_cmds_and_ave_vol_fps = newstack_fl_rc_cmd.map(
        fp=tomogram_fps, upstream_tasks=[clips]
    )
    ns_float_rc_cmds = utils.to_command.map(ns_float_rc_cmds_and_ave_vol_fps)
    ns_float_rcs = shell_task.map(
        command=ns_float_rc_cmds, to_echo=unmapped("rc float commands")
    )
    bin_vol_cmds_and_avebin8_fps = gen_binvol_rc_cmd.map(
        fp=tomogram_fps, upstream_tasks=[ns_float_rcs]
    )
    bin_vol_cmds = utils.to_command.map(bin_vol_cmds_and_avebin8_fps)
    bin_vols = shell_task.map(
        command=bin_vol_cmds, to_echo=unmapped("bin vols commands")
    )
    mrc2tiff_rc_cmds = gen_mrc2tiff_rc_cmd.map(
        fp=tomogram_fps, upstream_tasks=[bin_vols]
    )
    mrc2tiff_rcs = shell_task.map(
        command=mrc2tiff_rc_cmds, to_echo=unmapped("mrc2tiff_rcs commands")
    )
    ffmpeg_rc_cmds_and_ffmpeg_rc_fps = gen_ffmpeg_rc_cmd.map(
        fp=tomogram_fps, upstream_tasks=[mrc2tiff_rcs]
    )
    ffmpeg_rc_cmds = utils.to_command.map(ffmpeg_rc_cmds_and_ffmpeg_rc_fps)
    ffmpeg_rcs = shell_task.map(
        command=ffmpeg_rc_cmds, to_echo=unmapped("ffmpeg_rc_cmds commands")
    )
    # END RECONSTR MOVIE

    # START PYRAMID GEN
    mrc2nifti_cmds = gen_mrc2nifti_cmd.map(
        fp=tomogram_fps, upstream_tasks=[brts_ok]
    )
    mrc2niftis = shell_task.map(
        command=mrc2nifti_cmds, to_echo=unmapped("mrc2nifti")
    )
    pyramid_cmds_and_pyramid_fps = gen_pyramid_cmd.map(
        fp=tomogram_fps, upstream_tasks=[mrc2niftis]
    )
    pyramid_cmds = utils.to_command.map(pyramid_cmds_and_pyramid_fps)
    gen_pyramids = shell_task.map(
        command=pyramid_cmds, to_echo=unmapped("gen pyramid")
    )
    min_max_cmds_and_out_fps = gen_min_max_cmd.map(
        fp=tomogram_fps, upstream_tasks=[mrc2niftis]
    )
    min_max_cmds = utils.to_command.map(min_max_cmds_and_out_fps)
    min_maxs = shell_task.map(command=min_max_cmds, to_echo=unmapped("Min max"))
    metadatas = parse_min_max_file.map(
        l=min_max_cmds_and_out_fps, upstream_tasks=[min_maxs]
    )
    # END PYRAMID

    #        # create assets directory
    assets_dir = utils.make_assets_dir(input_dir=input_dir)

    # generate callback body
    files_elts = utils.generate_callback_files.map(input_fname=tomogram_fps)

    # keyThumbnail
    keyThumbnail_elts = utils.move_to_assets.map(
        cmd_and_fp=gm_cmds_and_thumbnail_locs,
        asset_dir=unmapped(assets_dir),
        input_fp=tomogram_fps,
        asset_type=unmapped("keyThumbnail"),
        upstream_tasks=[gms],
    )

    files_elts = utils.add_assets.map(
        assets_list=files_elts, new_asset=keyThumbnail_elts
    )

    # tiltMovie
    tiltMovie_elts = utils.move_to_assets.map(
        cmd_and_fp=mpeg_cmds_and_tiltMovie_fps,
        asset_dir=unmapped(assets_dir),
        input_fp=tomogram_fps,
        asset_type=unmapped("tiltMovie"),
        upstream_tasks=[mpegs],
    )
    files_elts = utils.add_assets.map(
        assets_list=files_elts, new_asset=tiltMovie_elts
    )

    # averagedVolume
    averagedVolume_elts = utils.move_to_assets.map(
        cmd_and_fp=ns_float_rc_cmds_and_ave_vol_fps,
        asset_dir=unmapped(assets_dir),
        input_fp=tomogram_fps,
        asset_type=unmapped("averagedVolume"),
        upstream_tasks=[ns_float_rcs],
    )
    files_elts = utils.add_assets.map(
        assets_list=files_elts, new_asset=averagedVolume_elts
    )

    # volume
    volume_elts = utils.move_to_assets.map(
        cmd_and_fp=bin_vol_cmds_and_avebin8_fps,
        asset_dir=unmapped(assets_dir),
        input_fp=tomogram_fps,
        asset_type=unmapped("volume"),
        upstream_tasks=[bin_vols],
    )
    files_elts = utils.add_assets.map(assets_list=files_elts, new_asset=volume_elts)

    # recMovie
    recMovie_elts = utils.move_to_assets.map(
        cmd_and_fp=ffmpeg_rc_cmds_and_ffmpeg_rc_fps,
        asset_dir=unmapped(assets_dir),
        input_fp=tomogram_fps,
        asset_type=unmapped("recMovie"),
        upstream_tasks=[ffmpeg_rcs],
    )
    files_elts = utils.add_assets.map(
        assets_list=files_elts, new_asset=recMovie_elts
    )

    # element neuroglancerPrecomputed
    ng_elts = utils.move_to_assets.map(
        cmd_and_fp=pyramid_cmds_and_pyramid_fps,
        asset_dir=unmapped(assets_dir),
        input_fp=tomogram_fps,
        asset_type=unmapped("neuroglancerPrecomputed"),
        upstream_tasks=[gen_pyramids],
        metadata=metadatas,
    )
    ng_elts = utils.add_assets.map(assets_list=files_elts, new_asset=ng_elts)

    utils.print_t.map(
        files_elts,
        upstream_tasks=[
            keyThumbnail_elts,
            tiltMovie_elts,
            averagedVolume_elts,
            volume_elts,
            recMovie_elts,
            ng_elts,
        ],
    )
# utils.print_t.map(min_maxs)


result = f.run(
    adoc_template="dirTemplate",
    dual="0",
    montage="0",
    gold="15",
    focus="0",
    bfocus="0",
    fiducialless="1",
    trackingMethod=None,
    TwoSurfaces="0",
    TargetNumberOfBeads="20",
    LocalAlignments="0",
    THICKNESS="30",
    input_dir="/brt_run/Projects/ABC2/",
    token="the_token",
    sample_id="the_sample_id",
    callback_url="https://ptsv2.com/t/",
)
# print(json.dumps(result.result[files_elts]))
