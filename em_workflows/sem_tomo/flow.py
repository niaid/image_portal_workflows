#!/usr/bin/env python3
"""
Sem Tomo pipeline overview:
---------------------------
    - Single directory is used to contain a set of gifs, which make up a single stack.
    - tifs compiled into a single mrc file (source.mrc) in convert_tif_to_mrc()
    - two metadata files (align xf, and align xg, are generated to allow creation of aligned mrc file (align.mrc)
    - another mrc file is created, which tries to correct for stage tilt (needs stretch file).

    This file is called corrected.mrc. Note, if no correction is needed, a correction.mrc is
    still created, but without any actual correction of angle.

    The corrected mrc file is then contrast adjusted with mean std dev magic numbers "150,40"
    in gen_newstack_norm_command() this is referred to as the base mrc
    - A movie is created using the base.mrc file.
    - the midpoint of that file is computed, and snapshots are created using this midpoint.
    - We now want to create the pyramid assets, for neuroglancer / viewer.
    - Firstly create nifti file using the base mrc, then convert this to ng format.
    - To conclude, send callback stating the location of the various outputs.
"""

import glob
import math
from pathlib import Path
from typing import Dict, Optional
from natsort import os_sorted

from prefect import flow, task, unmapped
from pytools.HedwigZarrImages import HedwigZarrImages

from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng
from em_workflows.file_path import FilePath
from em_workflows.constants import AssetType
from em_workflows.sem_tomo.config import SEMConfig
from em_workflows.sem_tomo.constants import FIBSEM_DEPTH, FIBSEM_HEIGHT, FIBSEM_WIDTH


@task
def gen_xfalign_comand(fp_in: FilePath) -> None:
    """
    eg::

        xfalign -pa -1 -pr source.mrc align.xf
    """
    source_mrc = fp_in.gen_output_fp(out_fname="source.mrc")
    if not source_mrc.exists():
        utils.log(f"{source_mrc} does not exist")
    align_xf = fp_in.gen_output_fp(out_fname="align.xf")
    log_file = f"{source_mrc.parent}/xfalign.log"
    utils.log(f"xfalign log_file: {log_file}")
    cmd = [
        SEMConfig.xfalign_loc,
        "-pa",
        "-1",
        "-pr",
        source_mrc.as_posix(),
        align_xf.as_posix(),
    ]
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_align_xg(fp_in: FilePath) -> None:
    """
    eg::

        xftoxg -ro -mi 2 {WORKDIR}/align.xf {WORKDIR}/align.xg
    """
    align_xg = fp_in.gen_output_fp(out_fname="align.xg")
    align_xf = fp_in.gen_output_fp(out_fname="align.xf")
    log_file = f"{align_xg.parent}/xgalign.log"
    cmd = [
        SEMConfig.xftoxg_loc,
        "-ro",
        "-mi",
        "2",
        align_xf.as_posix(),
        align_xg.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_newstack_combi(fp_in: FilePath) -> Dict:
    """
    eg::

        newstack -x align.xg -x stretch.xf -meansd 150,40 -mo 0 source.mrc out.mrc
    """
    align_xg = fp_in.gen_output_fp(out_fname="align.xg")
    stretch_xf = fp_in.gen_output_fp(out_fname="stretch.xf")
    source_mrc = fp_in.gen_output_fp(out_fname="source.mrc")
    base_mrc = fp_in.gen_output_fp(output_ext=".mrc", out_fname="adjusted.mrc")
    # output_fp = fp_in.gen_output_fp(out_fname="out.mrc")
    log_file = f"{align_xg.parent}/ns_align.log"
    cmd = [
        SEMConfig.newstack_loc,
        "-x",
        align_xg.as_posix(),
        "-x",
        stretch_xf.as_posix(),
        "-meansd",
        "150,40",
        "-mo",
        "0",
        source_mrc.as_posix(),
        base_mrc.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    assets_fp_adjusted_mrc = fp_in.copy_to_assets_dir(fp_to_cp=base_mrc)
    return fp_in.gen_asset(
        asset_type=AssetType.AVERAGED_VOLUME, asset_fp=assets_fp_adjusted_mrc
    )


@task
def convert_tif_to_mrc(file_path: FilePath) -> int:
    """
    | Generates source.mrc
    | assumes there's tifs in input dir, uses all the tifs in dir
    | eg::

        tif2mrc {DATAPATH}/*.tif {WORKDIR}/Source.mrc
    """
    output_fp = file_path.gen_output_fp(out_fname="source.mrc")
    log_file = f"{output_fp.parent}/tif2mrc.log"
    files = glob.glob(f"{file_path.fp_in.as_posix()}/*.tif")

    cmd = [SEMConfig.tif2mrc_loc]
    cmd.extend(os_sorted(files))
    cmd.append(output_fp.as_posix())
    utils.log(f"Created {cmd}")
    return_code = FilePath.run(cmd=cmd, log_file=log_file)
    return return_code


@task
def create_stretch_file(tilt: float, fp_in: FilePath) -> None:
    """
    | Creates stretch.xf used to gen corrected.mrc
    | File looks like:

    ``1 0 0 {TILT_PARAMETER} 0 0``

    | where TILT_PARAMETER is calculated as 1/cos({TILT_ANGLE}).
    | Note that tilt angle is input in degrees, however cos method expects radians
    """
    # math.cos expects radians, convert to degrees first.
    tilt_angle = 1 / math.cos(math.degrees(float(tilt)))
    utils.log(f"creating stretch file, tilt_angle: {tilt_angle}.")
    output_fp = fp_in.gen_output_fp(out_fname="stretch.xf")
    with open(output_fp.as_posix(), "w") as _file:
        _file.write(f"1 0 0 {tilt_angle} 0 0")


@task
def gen_newstack_mid_mrc_command(fp_in: FilePath) -> None:
    """
    Generates mid.mrc. eg::

        newstack -secs {MIDZ}-{MIDZ} {WORKDIR}/{BASENAME}.mrc {WORKDIR}/mid.mrc
    """
    mid_mrc = fp_in.gen_output_fp(out_fname="mid.mrc")
    base_mrc = fp_in.gen_output_fp(output_ext=".mrc", out_fname="adjusted.mrc")
    utils.log(fp_in.fp_in.as_posix())
    tifs = glob.glob(f"{fp_in.fp_in.as_posix()}/*tif")
    # tile names can be an issue with inputs - natsort seems to get things  ~right
    tifs_nat_sorted = os_sorted(tifs)
    utils.log(tifs_nat_sorted)
    mid_z = str(int(len(tifs_nat_sorted) / 2))
    log_file = f"{base_mrc.parent}/newstack_mid.log"
    cmd = [
        SEMConfig.newstack_loc,
        "-secs",
        mid_z,
        base_mrc.as_posix(),
        mid_mrc.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_keyimg(fp_in: FilePath) -> Dict:
    """
    Generates keyimg (large thumb), eg::

        mrc2tif -j -C 0,255 mid.mrc {WORKDIR}/keyimg_{BASENAME}.jpg
    """
    mid_mrc = fp_in.gen_output_fp(out_fname="mid.mrc")
    keyimg_fp = fp_in.gen_output_fp(out_fname="keyimg.jpg")
    log_file = f"{mid_mrc.parent}/mrc2tif.log"
    cmd = [
        SEMConfig.mrc2tif_loc,
        "-j",
        "-C",
        "0,255",
        mid_mrc.as_posix(),
        keyimg_fp.as_posix(),
    ]
    utils.log(f"Created keyimg {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = fp_in.copy_to_assets_dir(fp_to_cp=keyimg_fp)
    keyimg_asset = fp_in.gen_asset(asset_type=AssetType.KEY_IMAGE, asset_fp=asset_fp)
    return keyimg_asset


@task
def gen_keyimg_small(fp_in: FilePath) -> Dict:
    """
    eg::

        convert -size 300x300 {WORKDIR}/hedwig/keyimg_{BASENAME}.jpg \
                -resize 300x300 -sharpen 2 -quality 70 {WORKDIR}/hedwig/keyimg_{BASENAME}_s.jpg
    """
    keyimg_fp = fp_in.gen_output_fp(out_fname="keyimg.jpg")
    keyimg_sm_fp = fp_in.gen_output_fp(out_fname="keyimg_sm.jpg")
    log_file = f"{keyimg_sm_fp.parent}/convert.log"
    cmd = [
        SEMConfig.convert_loc,
        "-size",
        "300x300",
        keyimg_fp.as_posix(),
        "-resize",
        "300x300",
        "-sharpen",
        "2",
        "-quality",
        "70",
        keyimg_sm_fp.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = fp_in.copy_to_assets_dir(fp_to_cp=keyimg_sm_fp)
    keyimg_asset = fp_in.gen_asset(asset_type=AssetType.THUMBNAIL, asset_fp=asset_fp)
    return keyimg_asset


@task
def gen_zarr(fp_in: FilePath) -> None:
    file_path = fp_in
    # fallback mrc file
    input_file = file_path.fp_in.as_posix()

    base_mrc = file_path.gen_output_fp(output_ext=".mrc", out_fname="adjusted.mrc")
    if base_mrc.is_file():
        input_file = base_mrc.as_posix()

    ng.bioformats_gen_zarr(
        file_path=file_path,
        input_fname=input_file,
        depth=FIBSEM_DEPTH,
        width=FIBSEM_WIDTH,
        height=FIBSEM_HEIGHT,
        resolutions=1,
    )
    output_zarr = Path(f"{file_path.working_dir}/{file_path.base}.zarr")
    file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))

    ng.zarr_build_multiscales(fp_in)


@task
def gen_ng_metadata(fp_in: FilePath) -> Dict:
    """
    | Initialize HedwigZarrImages instace from .zarr group array
    | For each HedwigZarrImage obtain their metadata, consisting of:
    - shader_type
    - dimension
    - neuroglancer shader parameters

    | Note; the seemingly redundancy of working and asset fp here.
    | However asset fp is in the network file system and is deployed for access to the users
    | Working fp is actually used for getting the metadata
    """

    file_path = fp_in
    asset_fp = Path(f"{file_path.assets_dir}/{file_path.base}.zarr")
    working_fp = Path(f"{file_path.working_dir}/{file_path.base}.zarr")
    hw_images = HedwigZarrImages(zarr_path=working_fp, read_only=False)
    hw_image = hw_images[list(hw_images.get_series_keys())[0]]

    # NOTE: this could be replaced by hw_image.path
    # but hw_image is part of working dir (temporary)
    first_zarr_arr = asset_fp / "0"

    ng_asset = file_path.gen_asset(
        asset_type=AssetType.NEUROGLANCER_ZARR, asset_fp=first_zarr_arr
    )
    ng_asset["metadata"] = dict(
        shader=hw_image.shader_type,
        dimensions=hw_image.dims,
        shaderParameters=hw_image.neuroglancer_shader_parameters(mad_scale=5.0),
    )
    return ng_asset


@flow(
    name="SEM TOMO",
    flow_run_name=utils.generate_flow_run_name,
    log_prints=True,
    task_runner=SEMConfig.SLURM_EXECUTOR,
    on_completion=[utils.notify_api_completion],
    on_failure=[utils.notify_api_completion],
)
def sem_tomo_flow(
    file_share: str,
    input_dir: str,
    x_file_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    token: Optional[str] = None,
    x_no_api: bool = False,
    x_keep_workdir: bool = False,
    tilt_angle: float = 0,
):
    utils.notify_api_running(x_no_api, token, callback_url)

    input_dir_fp = utils.get_input_dir.submit(
        share_name=file_share, input_dir=input_dir
    )
    # note FIBSEM is different to other flows in that it uses *directories*
    # to define stacks. Therefore, will have to list dirs to discover stacks
    # (rather than eg mrc files)
    input_dir_fps = utils.list_dirs.submit(input_dir_fp=input_dir_fp)

    fps = utils.gen_fps.submit(
        share_name=file_share, input_dir=input_dir_fp, fps_in=input_dir_fps
    )
    tif_to_mrc = convert_tif_to_mrc.map(fps)

    # using source.mrc gen align.xf
    align_xfs = gen_xfalign_comand.map(fp_in=fps, wait_for=[tif_to_mrc])

    # using align.xf create align.xg
    align_xgs = gen_align_xg.map(fp_in=fps, wait_for=[align_xfs])

    # create stretch file using tilt_parameter
    stretchs = create_stretch_file.map(tilt=unmapped(tilt_angle), fp_in=fps)

    base_mrcs = gen_newstack_combi.map(fp_in=fps, wait_for=[stretchs, align_xgs])
    corrected_movie_assets = utils.mrc_to_movie.map(
        file_path=fps,
        root=unmapped("adjusted"),
        asset_type=unmapped(AssetType.REC_MOVIE),
        wait_for=[base_mrcs],
    )

    # generate midpoint mrc file
    mid_mrcs = gen_newstack_mid_mrc_command.map(fp_in=fps, wait_for=[base_mrcs])

    # large thumb
    keyimg_assets = gen_keyimg.map(fp_in=fps, wait_for=[mid_mrcs])
    # small thumb
    thumb_assets = gen_keyimg_small.map(fp_in=fps, wait_for=[keyimg_assets])

    # zarr file generation
    zarrs = gen_zarr.map(fp_in=fps, wait_for=[base_mrcs])
    pyramid_assets = gen_ng_metadata.map(fp_in=fps, wait_for=[zarrs])

    # this is the toplevel element (the input file basically) onto which
    # the "assets" (ie the outputs derived from this file) are hung.
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_keyimgs = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=keyimg_assets
    )
    callback_with_pyramids = utils.add_asset.map(
        prim_fp=callback_with_keyimgs, asset=pyramid_assets
    )
    callback_with_corr_mrcs = utils.add_asset.map(
        prim_fp=callback_with_pyramids, asset=base_mrcs
    )
    callback_with_corr_movies = utils.add_asset.map(
        prim_fp=callback_with_corr_mrcs, asset=corrected_movie_assets
    )

    utils.callback_with_cleanup(
        fps=fps,
        callback_result=callback_with_corr_movies,
        x_no_api=x_no_api,
        callback_url=callback_url,
        token=token,
        x_keep_workdir=x_keep_workdir,
    )
