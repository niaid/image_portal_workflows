#!/usr/bin/env python3
import glob
from em_workflows.file_path import FilePath
import subprocess
import re
import math
from typing import List

from pathlib import Path
from prefect import task, Flow, Parameter, unmapped
from prefect.run_configs import LocalRun
from prefect.engine import signals


from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng
from em_workflows.shell_task_echo import ShellTaskEcho
from em_workflows.config import Config

shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


@task
def gen_dimension_command(file_path: FilePath) -> str:
    ali_file = f"{file_path.working_dir}/{file_path.base}_ali.mrc"
    ali_file_p = Path(ali_file)
    if ali_file_p.exists():
        utils.log(f"{ali_file} exists")
    else:
        utils.log(f"{ali_file} DOES NOT exist")
        raise signals.FAIL(
            f"File {ali_file} does not exist. gen_dimension_command failure."
        )
    cmd = [Config.header_loc, "-s", ali_file]
    sp = subprocess.run(cmd, check=False, capture_output=True)
    if sp.returncode != 0:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"ERROR : {stderr} -- {stdout}"
        utils.log(msg)
        raise signals.FAIL(msg)
    else:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"Command ok : {stderr} -- {stdout}"
        utils.log(msg)
        xyz_dim = re.split(" +(\d+)", stdout)
        z_dim = xyz_dim[5]
        utils.log(f"z_dim: {z_dim:}")
        return z_dim


@task
def gen_ali_x(file_path: FilePath, z_dim) -> None:
    """
    newstack -secs {i}-{i} path/BASENAME_ali*.mrc WORKDIR/hedwig/BASENAME_ali{i}.mrc
    """
    ali_file = f"{file_path.working_dir}/{file_path.base}_ali.mrc"
    for i in range(1, int(z_dim)):
        i_padded = str(i).rjust(3, "0")
        ali_x = f"{file_path.working_dir}/{file_path.base}_ali{i_padded}.mrc"
        log_file = f"{file_path.working_dir}/newstack_mid_pt.log"
        cmd = [Config.newstack_loc, "-secs", f"{i}-{i}", ali_file, ali_x]
        FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_ali_asmbl(file_path: FilePath) -> None:
    """
    newstack -float 3 {BASENAME}_ali*.mrc ali_{BASENAME}.mrc
    """
    alis = glob.glob(f"{file_path.working_dir}/{file_path.base}_ali*.mrc")
    ali_asmbl = f"{file_path.working_dir}/ali_{file_path.base}.mrc"
    ali_base_cmd = [Config.newstack_loc, "-float", "3"]
    ali_base_cmd.extend(alis)
    ali_base_cmd.append(ali_asmbl)
    FilePath.run(cmd=ali_base_cmd, log_file=f"{file_path.working_dir}/asmbl.log")


@task
def gen_mrc2tiff(file_path: FilePath) -> None:
    """
    mrc2tif -j -C 0,255 ali_BASENAME.mrc BASENAME_ali
    """
    ali_asmbl = f"{file_path.working_dir}/ali_{file_path.base}.mrc"
    ali = f"{file_path.working_dir}/{file_path.base}_ali"
    cmd = [Config.mrc2tif_loc, "-j", "-C", "0,255", ali_asmbl, ali]
    log_file = f"{file_path.working_dir}/mrc2tif_align.log"
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_thumbs(file_path: FilePath, z_dim) -> dict:
    """
    gm convert -size 300x300 BASENAME_ali.{MIDDLE_I}.jpg -resize 300x300 -sharpen 2 -quality 70 keyimg_BASENAME_s.jpg
    """
    middle_i = calc_middle_i(z_dim=z_dim)
    middle_i_jpg = f"{file_path.working_dir}/{file_path.base}_ali.{middle_i}.jpg"
    thumb = f"{file_path.working_dir}/keyimg_{file_path.base}_s.jpg"
    cmd = [
        "gm",
        "convert",
        "-size",
        "300x300",
        middle_i_jpg,
        "-resize",
        "300x300",
        "-sharpen",
        "2",
        "-quality",
        "70",
        thumb,
    ]
    log_file = f"{file_path.working_dir}/thumb.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(thumb))
    keyimg_asset = file_path.gen_asset(asset_type="thumbnail", asset_fp=asset_fp)
    return keyimg_asset


@task
def gen_copy_keyimages(file_path: FilePath, z_dim: str) -> dict:
    """
    generates the keyImage (by copying image i to keyImage.jpeg)
    fname_in and fname_out both derived from tomogram fp
    MIDDLE_I might always be an int
    cp BASENAME_ali.{MIDDLE_I}.jpg BASENAME_keyimg.jpg
    """
    middle_i = calc_middle_i(z_dim=z_dim)
    middle_i_jpg = f"{file_path.working_dir}/{file_path.base}_ali.{middle_i}.jpg"
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(middle_i_jpg))
    keyimg_asset = file_path.gen_asset(asset_type="keyImage", asset_fp=asset_fp)
    return keyimg_asset


def calc_middle_i(z_dim: str) -> str:
    """
    we want to find the middle image of the stack (for use as thumbnail)
    the file name later needed is padded with zeros
    TODO - this might be 3 or 4 - make this not a magic number.
    """
    fl = math.floor(int(z_dim) / 2)
    fl_padded = str(fl).rjust(3, "0")
    utils.log(f"middle i: {fl_padded}")
    return fl_padded


@task
def gen_tilt_movie(file_path: FilePath) -> dict:
    """
    generates the tilt moveie
    ffmpeg -f image2 -framerate 4 -i ${BASENAME}_ali.%03d.jpg -vcodec \
            libx264 -pix_fmt yuv420p -s 1024,1024 tiltMov_${BASENAME}.mp4
    """
    input_fp = f"{file_path.working_dir}/{file_path.base}_ali.%03d.jpg"
    log_file = f"{file_path.working_dir}/ffmpeg_tilt.log"
    movie_file = f"{file_path.working_dir}/tiltMov_{file_path.base}.mp4"
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "image2",
        "-framerate",
        "4",
        "-i",
        input_fp,
        "-vcodec",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-s",
        "1024,1024",
        movie_file,
    ]
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(movie_file))
    tilt_movie_asset = file_path.gen_asset(asset_type="tiltMovie", asset_fp=asset_fp)
    return tilt_movie_asset


@task
def gen_recon_movie(file_path: FilePath) -> dict:
    """
    ffmpeg -f image2 -framerate 8 -i WORKDIR/hedwig/BASENAME_mp4.%04d.jpg -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 WORKDIR/hedwig/keyMov_BASENAME.mp4
    """
    mp4_input = f"{file_path.working_dir}/{file_path.base}_mp4.%03d.jpg"
    key_mov = f"{file_path.working_dir}/{file_path.base}_keyMov.mp4"
    cmd = [
        "ffmpeg",
        "-f",
        "image2",
        "-framerate",
        "8",
        "-i",
        mp4_input,
        "-vcodec",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-s",
        "1024,1024",
        key_mov,
    ]
    log_file = f"{file_path.working_dir}/{file_path.base}_keyMov.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(key_mov))
    recon_movie_asset = file_path.gen_asset(asset_type="recMovie", asset_fp=asset_fp)
    return recon_movie_asset


@task
def gen_clip_avgs(file_path: FilePath, z_dim: str) -> None:
    """
    for i in range(2, dimensions.z-2):
      IZMIN = i-2
      IZMAX = i+2
      clip avg -2d -iz IZMIN-IZMAX  -m 1 BASENAME_rec.mrc BASENAME_ave${I}.mrc
    """
    for i in range(2, int(z_dim) - 2):
        izmin = i - 2
        izmax = i + 2
        in_fp = f"{file_path.working_dir}/{file_path.base}_rec.mrc"
        padded_val = str(i).zfill(3)
        ave_mrc = f"{file_path.working_dir}/{file_path.base}_ave{padded_val}.mrc"
        min_max = f"{str(izmin)}-{str(izmax)}"
        cmd = [Config.clip_loc, "avg", "-2d", "-iz", min_max, "-m", "1", in_fp, ave_mrc]
        log_file = f"{file_path.working_dir}/clip_avg.error.log"
        FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_ave_vol(file_path: FilePath) -> dict:
    """
    newstack -float 3 BASENAME_ave* ave_BASENAME.mrc
    """
    aves = glob.glob(f"{file_path.working_dir}/{file_path.base}_ave*")
    ave_mrc = f"{file_path.working_dir}/{file_path.base}_ave.mrc"
    cmd = [Config.newstack_loc, "-float", "3"]
    cmd.extend(aves)
    cmd.append(ave_mrc)
    log_file = f"{file_path.working_dir}/newstack_float.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(ave_mrc))
    ave_vol_asset = file_path.gen_asset(asset_type="averagedVolume", asset_fp=asset_fp)
    return ave_vol_asset


@task
def gen_ave_8_vol(file_path: FilePath):
    """
    binvol -binning 2 WORKDIR/hedwig/ave_BASENAME.mrc WORKDIR/hedwig/avebin8_BASENAME.mrc
    """
    ave_8_mrc = f"{file_path.working_dir}/avebin8_{file_path.base}.mrc"
    ave_mrc = f"{file_path.working_dir}/{file_path.base}_ave.mrc"
    cmd = [Config.binvol, "-binning", "2", ave_mrc, ave_8_mrc]
    log_file = f"{file_path.working_dir}/ave_8_mrc.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(ave_8_mrc))
    bin_vol_asset = file_path.gen_asset(asset_type="volume", asset_fp=asset_fp)
    return bin_vol_asset


@task
def gen_mrc2tif(file_path: FilePath):
    """
    mrc2tif -j -C 100,255 WORKDIR/hedwig/ave_BASNAME.mrc hedwig/BASENAME_mp4
    """
    mp4 = f"{file_path.working_dir}/{file_path.base}_mp4"
    ave_8_mrc = f"{file_path.working_dir}/avebin8_{file_path.base}.mrc"
    log_file = f"{file_path.working_dir}/recon_mrc2tiff.log"
    cmd = [Config.mrc2tif_loc, "-j", "-C", "100,255", ave_8_mrc, mp4]
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def list_paired_files(fnames: List[Path]) -> List[Path]:
    """
    THIS IS AN OLD FUNC : Keeping for now, they'll probably want this back.
    if the input is a paired/dual axis shot, we can trucate the a|b off
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


#@task
#def check_inputs_paired(fps: List[Path]):
#    """
#    THIS IS AN OLD FUNC : Keeping for now, they'll probably want this back.
#    asks if there are ANY paired inputs in a dir.
#    If there are, will return True, else False
#    """
#    fnames = [fname.stem for fname in fps]
#    inputs_paired = False
#    for fname in fnames:
#        if fname.endswith("a"):
#            # remove the last char, cat on a 'b' and lookup.
#            pair_name = fname[:-1] + "b"
#            if pair_name in fnames:
#                inputs_paired = True
#    utils.log(f"Are inputs paired? {inputs_paired}.")
#    return inputs_paired



with Flow(
    "brt_flow",
    executor=Config.SLURM_EXECUTOR,
    state_handlers=[utils.notify_api_running],
    terminal_state_handler=utils.custom_terminal_state_handler,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:

    # This block of params map are for adoc file specfication.
    # Note the ugly names, these parameters are lifted verbatim from
    # https://bio3d.colorado.edu/imod/doc/directives.html where possible.
    # (there are two thickness args, these are not verbatim.)
    montage = Parameter("montage")
    gold = Parameter("gold")
    focus = Parameter("focus")
    fiducialless = Parameter("fiducialless")
    trackingMethod = Parameter("trackingMethod")
    TwoSurfaces = Parameter("TwoSurfaces")
    TargetNumberOfBeads = Parameter("TargetNumberOfBeads")
    LocalAlignments = Parameter("LocalAlignments")
    THICKNESS = Parameter("THICKNESS")
    # end user facing adoc params

    adoc_template = Parameter("adoc_template", default="plastic_brt")
    input_dir = Parameter("input_dir")
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    file_name = Parameter("file_name", default=None)
    # a single input_dir will have n tomograms
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    # input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    input_fps = utils.list_files(
        input_dir=input_dir_fp, exts=["MRC", "ST", "mrc", "st"], single_file=file_name
    )

    fps = utils.gen_fps(input_dir=input_dir_fp, fps_in=input_fps)
    brts = utils.run_brt.map(
        file_path=fps,
        adoc_template=unmapped(adoc_template),
        montage=unmapped(montage),
        gold=unmapped(gold),
        focus=unmapped(focus),
        fiducialless=unmapped(fiducialless),
        trackingMethod=unmapped(trackingMethod),
        TwoSurfaces=unmapped(TwoSurfaces),
        TargetNumberOfBeads=unmapped(TargetNumberOfBeads),
        LocalAlignments=unmapped(LocalAlignments),
        THICKNESS=unmapped(THICKNESS),
    )
    # END BRT, check files for success (else fail here)

    # stack dimensions - used in movie creation
    z_dims = gen_dimension_command.map(file_path=fps, upstream_tasks=[brts])

    # START TILT MOVIE GENERATION:
    ali_xs = gen_ali_x.map(file_path=fps, z_dim=z_dims, upstream_tasks=[brts])
    asmbls = gen_ali_asmbl.map(file_path=fps, upstream_tasks=[ali_xs])
    mrc2tiffs = gen_mrc2tiff.map(file_path=fps, upstream_tasks=[asmbls])
    thumb_assets = gen_thumbs.map(
        file_path=fps, z_dim=z_dims, upstream_tasks=[mrc2tiffs]
    )
    keyimg_assets = gen_copy_keyimages.map(
        file_path=fps, z_dim=z_dims, upstream_tasks=[mrc2tiffs]
    )
    tilt_movie_assets = gen_tilt_movie.map(
        file_path=fps, upstream_tasks=[keyimg_assets]
    )
    # END TILT MOVIE GENERATION

    # START RECONSTR MOVIE GENERATION:
    clip_avgs = gen_clip_avgs.map(file_path=fps, z_dim=z_dims, upstream_tasks=[asmbls])
    averagedVolume_assets = gen_ave_vol.map(file_path=fps, upstream_tasks=[clip_avgs])
    bin_vol_assets = gen_ave_8_vol.map(
        file_path=fps, upstream_tasks=[averagedVolume_assets]
    )
    mrc2tiffs = gen_mrc2tif.map(file_path=fps, upstream_tasks=[bin_vol_assets])
    recon_movie_assets = gen_recon_movie.map(file_path=fps, upstream_tasks=[mrc2tiffs])
    #    # END RECONSTR MOVIE
    #
    #    # START PYRAMID GEN
    # nifti file generation
    niftis = ng.gen_niftis.map(fp_in=fps, upstream_tasks=[brts])
    pyramid_assets = ng.gen_pyramids.map(fp_in=fps, upstream_tasks=[niftis])
    archive_pyramid_cmds = ng.gen_archive_pyr.map(
        file_path=fps, upstream_tasks=[pyramid_assets]
    )

    # now we've done the computational work.
    # the relevant files have been put into the Assets dirs, but we need to inform the API
    # Generate a base "primary path" dict, and hang dicts onto this.
    # repeatedly pass asset in to add_asset func to add asset in question.
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_keyimgs = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=keyimg_assets
    )
    callback_with_pyramids = utils.add_asset.map(
        prim_fp=callback_with_keyimgs, asset=pyramid_assets
    )
    callback_with_ave_vol = utils.add_asset.map(
        prim_fp=callback_with_pyramids, asset=averagedVolume_assets
    )
    callback_with_bin_vol = utils.add_asset.map(
        prim_fp=callback_with_ave_vol, asset=bin_vol_assets
    )
    callback_with_recon_mov = utils.add_asset.map(
        prim_fp=callback_with_bin_vol, asset=recon_movie_assets
    )
    callback_with_tilt_mov = utils.add_asset.map(
        prim_fp=callback_with_recon_mov, asset=tilt_movie_assets
    )

    # this is a bit dubious. Previously I wanted to ONLY copy intermed files upon failure.
    # now we copy evreything, everytime. :shrug emoji:
    # spoiler: (we're going to run out of space).
    cp_wd_to_assets = utils.copy_workdirs.map(
        fps, upstream_tasks=[callback_with_recon_mov]
    )
    # finally convert to JSON and send.
    cb = utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=callback_with_tilt_mov
    )
