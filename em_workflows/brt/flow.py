#!/usr/bin/env python3
"""
Batchruntomo pipeline overview:
-------------------------------
- Takes an ``input_dir`` containing 1 or more `.mrc` or `.st` files
- IMOD ``batchruntomo`` (BRT) https://bio3d.colorado.edu/imod/doc/man/batchruntomo.html is run on each file in dir.
- BRT has a large number of parameters, which are provided using an .adoc parameter file.
- Some of these parameters are provided to the pipeline on a per run basis, the remaining of these parameters are
  defaulted using the templated adoc file.
- There are two templates, ``../templates/plastic_brt.adoc`` and ``../templates/cryo_brt.adoc`` (Note, currently
  these are identical, we expect this to change.) The template chosen is defined by parameter adoc_template.
- The run time parameters are interpolated into the template, and this file is then run with BRT.
- BRT produces two output files that we care about: `_ali.mrc` and `_rec.mrc`
- Using the `ali` (alignment) file we generate a tilt movie:
    - look up dimensionality of alignment file, and generate n sections in ``gen_ali_x()``
    - we assemble these sections into a single aligned mrc file, in ``gen_ali_asmbl()``
    - we convert this into a stack of jpegs, in ``gen_mrc2tiff()``
    - we compile these jpegs into a tilt movie in ``gen_tilt_movie()``
    - ADDITIONALLY:
    - we take the mid point of the stack jpeg to use as the display thumbnail.
    - we clean up intermediate files now.
- Using the _rec (reconstructed) file we generate a reconstructed movie (in a similar fashion to the above)
    - look up dimensionality of the reconstructed (_rec) file,
    - create a stack of averaged mrc files in ``gen_clip_avgs()``
    - create single mrc using the above averaged stack, in ``consolidate_ave_mrcs()``
    - convert this mrc to a stack of jpegs, in ``gen_ave_jpgs_from_ave_mrc()``
    - compile jpegs into reconstructed movie, in ``gen_recon_movie()``
    - clean up after ourselves in ``cleanup_files()``
- Use average (created above) reconstructed mrc file to create input for volslicer.k in ``gen_ave_8_vol()``
- need to produce pyramid files with reconstructed mrc.
    - convert mrc file to Zarr, in gen_zarr()
- Now we need to copy the outputs to the right place, and tell the API where they are. We use JSON to talk to the API.
- build a json datastructure, containing the locations of the inputs we key on "primaryFilePath", and we append every
  output that's generated for *this* input into the "assets" json key.
- Finally, we ``POST`` the JSON datastructure to the API, and cleanup temp dirs.
"""

from typing import Dict
import json
import glob
import os
import subprocess
from typing import Optional
from pathlib import Path
from natsort import os_sorted
from prefect import task, flow, unmapped
from prefect.states import Completed, Failed
from pytools.HedwigZarrImages import HedwigZarrImages

from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng
from em_workflows.constants import AssetType
from em_workflows.file_path import FilePath
from em_workflows.brt.config import BRTConfig
from em_workflows.brt.constants import BRT_DEPTH, BRT_HEIGHT, BRT_WIDTH


def gen_dimension_command(fp_in: Path) -> str:
    """
    | looks up the z dimension of an mrc file.
    | ali_or_rec is nasty, str to denote whether you're using the `_ali` file or the `_rec` file.

    :todo: this is duplicate, see utils.lookup_dims()
    """

    if fp_in.exists():
        utils.log(f"{fp_in} exists")
    else:
        utils.log(f"{fp_in} DOES NOT exist, nothing to do here.")
        return "error"
    cmd = [BRTConfig.header_loc, "-s", fp_in]
    sp = subprocess.run(cmd, check=False, capture_output=True)
    if sp.returncode != 0:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"ERROR : {stderr} -- {stdout}"
        utils.log(msg)
        raise RuntimeError(msg)
    else:
        stdout = sp.stdout.decode("utf-8")
        stderr = sp.stderr.decode("utf-8")
        msg = f"Command ok : {stderr} -- {stdout}"
        utils.log(msg)
        xyz_dim = [int(x) for x in stdout.split()]
        z_dim = xyz_dim[2]
        utils.log(f"z_dim: {z_dim:}")
        return str(z_dim)


def gen_ali_x(fp_in: Path, z_dim) -> None:
    """
    - chops an mrc input into its constituent Z sections.
    - eg if an mrc input has a z_dim of 10, 10 sections will be generated.
    - the i-i syntax is awkward, and may not be required. Eg possibly replace i-i with i.
    - eg::

        newstack -secs {i}-{i} path/BASENAME_ali*.mrc WORKDIR/hedwig/BASENAME_ali{i}.mrc
    """
    for i in range(1, int(z_dim)):
        i_padded = str(i).rjust(3, "0")
        ali_x = f"{fp_in.parent}/{fp_in.stem}_align_{i_padded}.mrc"
        log_file = f"{fp_in.parent}/newstack_mid_pt.log"
        cmd = [BRTConfig.newstack_loc, "-secs", f"{i}-{i}", fp_in.as_posix(), ali_x]
        FilePath.run(cmd=cmd, log_file=log_file)


def gen_ali_asmbl(fp_in: Path) -> None:
    """
    Use IMOD ``newstack`` to assemble, eg::

       newstack -float 3 {BASENAME}_ali*.mrc ali_{BASENAME}.mrc
    """
    alis = glob.glob(f"{fp_in.parent}/{fp_in.stem}_align_*.mrc")
    alis.sort()
    ali_asmbl = f"{fp_in.parent}/ali_{fp_in.stem}.mrc"
    ali_base_cmd = [BRTConfig.newstack_loc, "-float", "3"]
    ali_base_cmd.extend(alis)
    ali_base_cmd.append(ali_asmbl)
    FilePath.run(cmd=ali_base_cmd, log_file=f"{fp_in.parent}/asmbl.log")


def gen_mrc2tiff(fp_in: Path) -> None:
    """
    This generates a lot of jpegs (-j) which will be compiled into a movie.
    (That is, the -jpeg switch is set to produce jpegs) eg::

        mrc2tif -j -C 0,255 ali_BASENAME.mrc BASENAME_ali
    """
    ali_asmbl = f"{fp_in.parent}/ali_{fp_in.stem}.mrc"
    ali = f"{fp_in.parent}/{fp_in.stem}_ali"
    cmd = [BRTConfig.mrc2tif_loc, "-j", "-C", "0,255", ali_asmbl, ali]
    log_file = f"{fp_in.parent}/mrc2tif_align.log"
    FilePath.run(cmd=cmd, log_file=log_file)


@task(
    name="Thumbnail generation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_thumbs(middle_i_jpg: Path) -> Path:
    """
    Use GraphicsMagick to create thumbnail images, eg::

        gm convert -size 300x300 BASENAME_ali.{MIDDLE_I}.jpg -resize 300x300 \
                -sharpen 2 -quality 70 keyimg_BASENAME_s.jpg
    """
    thumb = f"{middle_i_jpg.parent}/keyimg_{middle_i_jpg.stem}_s.jpg"
    cmd = [
        BRTConfig.gm_loc,
        "convert",
        "-size",
        "300x300",
        middle_i_jpg.as_posix(),
        "-resize",
        "300x300",
        "-sharpen",
        "2",
        "-quality",
        "70",
        thumb,
    ]
    log_file = f"{middle_i_jpg.parent}/thumb.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    return Path(thumb)


@task
def find_middle_image(fp_in: Path) -> Path:
    images = glob.glob(f"{fp_in.parent}/*ali*jpg")
    images_nat_sorted = os_sorted(images)
    middle_image = images_nat_sorted[int(len(images_nat_sorted) / 2)]
    utils.log(f"Found middle image {middle_image}")
    fp_out = Path(middle_image)
    cleanup_files(file_path=fp_in, pattern="*ali*jpg", keep_file=fp_out)
    return fp_out


@task(
    name="Tilt movie generation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_tilt_movie(brt_output: utils.BrtOutput) -> Path:
    """
    generates the tilt movie, eg::

        ffmpeg -f image2 -framerate 4 -i ${BASENAME}_ali.%03d.jpg -vcodec libx264 \
                -pix_fmt yuv420p -s 1024,1024 tiltMov_${BASENAME}.mp4
    """
    ali_file = brt_output.ali_file
    utils.log(f"created alinment file {ali_file}")
    utils.log("gen dims")
    z_dim = gen_dimension_command(fp_in=ali_file)

    utils.log("align x")
    gen_ali_x(fp_in=ali_file, z_dim=z_dim)

    utils.log("assemble x")
    gen_ali_asmbl(fp_in=ali_file)

    utils.log("mrc2tif")
    gen_mrc2tiff(fp_in=ali_file)

    input_fp = f"{ali_file.parent}/{ali_file.stem}_ali.%03d.jpg"
    log_file = f"{ali_file.parent}/ffmpeg_tilt.log"
    movie_file = f"{ali_file.parent}/tiltMov_{ali_file.stem}.mp4"
    cmd = [
        BRTConfig.ffmpeg_loc,
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
    cleanup_files(file_path=ali_file, pattern=("*_align_*.mrc"))
    return Path(movie_file)


@task(
    name="Average mrc generation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_ave_mrc(brt_output: utils.BrtOutput) -> Path:
    rec_file = brt_output.rec_file
    utils.log("gen recon dims")
    rec_z_dim = gen_dimension_command(fp_in=rec_file)
    utils.log("gen recon clip averages")
    gen_clip_avgs(in_fp=rec_file, z_dim=rec_z_dim)
    utils.log("gen average mrc")
    ave_mrc = consolidate_ave_mrcs(fp_in=rec_file)
    utils.log(f"average mrc: {ave_mrc}")
    return ave_mrc


@task(
    name="Movie compilation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_recon_movie(ave_mrc: Path) -> Path:
    """
    compiles a stack of jpgs into a movie. eg::

        ffmpeg -f image2 -framerate 8 -i WORKDIR/hedwig/BASENAME_mp4.%04d.jpg -vcodec libx264 \
                -pix_fmt yuv420p -s 1024,1024 WORKDIR/hedwig/keyMov_BASENAME.mp4

    """
    # gen_ave_jpgs_from_ave_mrc(ave_mrc=ave_mrc)
    mp4_base = f"{ave_mrc.parent}/{ave_mrc.stem}_mp4"
    mrc2tiff_log_file = f"{ave_mrc.parent}/recon_mrc2tiff.log"
    mrc2tiff_cmd = [
        BRTConfig.mrc2tif_loc,
        "-j",
        "-C",
        "100,255",
        ave_mrc.as_posix(),
        mp4_base,
    ]
    FilePath.run(cmd=mrc2tiff_cmd, log_file=mrc2tiff_log_file)
    # don't put the 's in here, as per docs. subprocess messes them up
    jpg_input_pattern = f"{mp4_base}*.jpg"
    key_mov = f"{ave_mrc.parent}/{ave_mrc.stem}_keyMov.mp4"
    cmd = [
        BRTConfig.ffmpeg_loc,
        "-f",
        "image2",
        "-pattern_type",
        "glob",
        "-framerate",
        "8",
        "-i",
        jpg_input_pattern,
        "-vcodec",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-s",
        "1024,1024",
        key_mov,
    ]
    log_file = f"{ave_mrc.parent}/{ave_mrc.stem}_keyMov.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    cleanup_files(file_path=ave_mrc, pattern=("_mp4.*.jpg"))
    return Path(key_mov)


def gen_clip_avgs(in_fp: Path, z_dim: str) -> None:
    """
    - give _rec mrc file, generate a stack of mrcs, averaged to assist viewing.
    - produces base_ave001.mrc etc, base_ave002.mrc etc,
    - inputs for newstack (for recon movie) and binvol (for volslicer)

    eg::

        for i in range(2, dimensions.z-2):
            IZMIN = i-2
            IZMAX = i+2
            clip avg -2d -iz IZMIN-IZMAX  -m 1 BASENAME_rec.mrc BASENAME_ave${i}.mrc
    """
    for i in range(2, int(z_dim) - 2):
        izmin = i - 2
        izmax = i + 2
        padded_val = str(i).zfill(4)
        ave_mrc = f"{in_fp.parent}/{in_fp.stem}_ave{padded_val}.mrc"
        min_max = f"{str(izmin)}-{str(izmax)}"
        cmd = [
            BRTConfig.clip_loc,
            "avg",
            "-2d",
            "-iz",
            min_max,
            "-m",
            "1",
            in_fp.as_posix(),
            ave_mrc,
        ]
        log_file = f"{in_fp.parent}/clip_avg.error.log"
        FilePath.run(cmd=cmd, log_file=log_file)


def consolidate_ave_mrcs(fp_in: Path) -> Path:
    """
    - consumes base_ave001.mrc etc, base_ave002.mrc etc,
    - creates ave_base.mrc the (averagedVolume asset)
    - eg::

        newstack -float 3 BASENAME_ave* ave_BASENAME.mrc
    """
    aves = glob.glob(f"{fp_in.parent}/{fp_in.stem}_ave*")
    aves.sort()
    ave_mrc = Path(f"{fp_in.parent}/ave_{fp_in.stem}.mrc")
    cmd = [BRTConfig.newstack_loc, "-float", "3"]
    cmd.extend(aves)
    cmd.append(ave_mrc.as_posix())
    log_file = f"{fp_in.parent}/newstack_float.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    cleanup_files(file_path=ave_mrc, pattern="*_ave*.mrc", keep_file=ave_mrc)
    return ave_mrc


@task(
    name="Volume asset creation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_ave_8_vol(ave_mrc: Path) -> Path:
    """
    - creates volume asset, for volslicer, eg::

        binvol -binning 2 WORKDIR/hedwig/ave_BASENAME.mrc WORKDIR/avebin8_BASENAME.mrc
    """
    ave_8_mrc = f"{ave_mrc.parent}/avebin8_{ave_mrc.stem}.mrc"
    cmd = [BRTConfig.binvol, "-binning", "2", ave_mrc.as_posix(), ave_8_mrc]
    log_file = f"{ave_mrc.parent}/ave_8_mrc.log"
    FilePath.run(cmd=cmd, log_file=log_file)
    return Path(ave_8_mrc)


def gen_ave_jpgs_from_ave_mrc(ave_mrc: Path):
    """
    - generates a load of jpgs from the ave_base.mrc with the format {base}_mp4.123.jpg \
            **OR** {base}_mp4.1234.jpg depending on size of stack.
    - These jpgs can later be compiled into a movie. eg::

        mrc2tif -j -C 100,255 WORKDIR/hedwig/ave_BASNAME.mrc hedwig/BASENAME_mp4
    """
    mp4 = f"{ave_mrc.parent}/{ave_mrc.stem}_mp4"
    log_file = f"{ave_mrc.parent}/recon_mrc2tiff.log"
    cmd = [BRTConfig.mrc2tif_loc, "-j", "-C", "100,255", ave_mrc.as_posix(), mp4]
    FilePath.run(cmd=cmd, log_file=log_file)


def cleanup_files(file_path: Path, pattern=str, keep_file: Path = None):
    """
    Given a ``FilePath`` and unix file ``pattern``, iterate through directory removing all files
    that match the pattern
    """
    f = f"{file_path.parent.as_posix()}/{pattern}"
    utils.log(f"trying to rm {f}")
    files_to_rm = glob.glob(f)
    for _file in files_to_rm:
        if keep_file and keep_file.as_posix() == _file:
            continue
        os.remove(_file)
    print(files_to_rm)


# @task
# def list_paired_files(fnames: List[Path]) -> List[Path]:
#     """
#     **THIS IS AN OLD FUNC** : Keeping for now, they'll probably want this back.
#
#     If the input is a paired/dual axis shot, we can trucate the ``a|b`` off
#     the filename, and use that string from this point on.
#     We need to ensure there are only paired inputs in this list.
#     """
#     maybes = list()
#     pairs = list()
#     # paired_fnames = [fname.stem for fname in fps]
#     for fname in fnames:
#         if fname.stem.endswith("a"):
#             # remove the last char of fname (keep extension)
#             fname_no_a = f"{fname.parent}/{fname.stem[:-1]}{fname.suffix}"
#             maybes.append(fname_no_a)
#     for fname in fnames:
#         if fname.stem.endswith("b"):
#             fname_no_b = f"{fname.parent}/{fname.stem[:-1]}{fname.suffix}"
#             if fname_no_b in maybes:
#                 pairs.append(Path(fname_no_b))
#     return pairs
#
#
# @task
# def check_inputs_paired(fps: List[Path]):
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


@task(
    name="Zarr generation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_zarr(brt_output: utils.BrtOutput) -> Path:

    if not brt_output.rec_file.is_file():
        raise ValueError(f"{brt_output.rec_file} does not exist")

    output_zarr = ng.bioformats_gen_zarr_dup(
        fp_in=brt_output.rec_file,
        depth=BRT_DEPTH,
        width=BRT_WIDTH,
        height=BRT_HEIGHT,
        resolutions=1,
    )
    ng.zarr_build_multiscales2(output_zarr)
    return output_zarr


@task
def copy_asset_gen_elt(file_path: FilePath, fp_to_cp: Path, asset_type: str) -> dict:
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=fp_to_cp)
    asset_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_fp)
    return asset_elt


@task(
    name="Neuroglancer metadata generation",
    on_failure=[utils.collect_exception_task_hook],
)
def gen_ng_metadata(fp_in: FilePath, zarr: Path) -> Dict:
    # Note; the seemingly redundancy of working and asset fp here.
    # However asset fp is in the network file system and is deployed for access to the users
    # Working fp is actually used for getting the metadata

    file_path = fp_in
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(zarr))

    utils.log("Instantiating HWZarrImages")
    hw_images = HedwigZarrImages(zarr_path=zarr, read_only=False)
    utils.log("Accessing first HWZarrImage")
    hw_image = hw_images[list(hw_images.get_series_keys())[0]]

    # NOTE: this could be replaced by hw_image.path
    # but hw_image is part of working dir (temporary)
    first_zarr_arr = asset_fp / "0"

    ng_asset = file_path.gen_asset(
        asset_type=AssetType.NEUROGLANCER_ZARR, asset_fp=first_zarr_arr
    )
    utils.log("Creating ng metadata")
    utils.log("... getting shader type")
    htype = hw_image.shader_type
    utils.log("... getting dims")
    hdims = hw_image.dims
    utils.log("... getting shader params")
    hparams = hw_image.neuroglancer_shader_parameters(mad_scale=5.0)
    ng_asset["metadata"] = {
        "shader": htype,
        "dimensions": hdims,
        "shaderParameters": hparams,
    }
    utils.log("DONE!!!")
    return ng_asset


@task
def get_callback_result(callback_data: list) -> list:
    cb_data = list()
    for item in callback_data:
        try:
            json.dumps(item)
            cb_data.append(item)
        except TypeError:  # can't serialize the item
            utils.log(f"Following item cannot be added to callback:\n\n{item}")
    return cb_data


@flow(
    name="BRT",
    flow_run_name=utils.generate_flow_run_name,
    log_prints=True,
    task_runner=BRTConfig.HIGH_SLURM_EXECUTOR,
    on_completion=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_failure=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_crashed=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
)
def brt_flow(
    # This block of params map are for adoc file specfication.
    # Note the ugly names, these parameters are lifted verbatim from
    # https://bio3d.colorado.edu/imod/doc/directives.html where possible.
    # (there are two thickness args, these are not verbatim.)
    montage: int,
    gold: int,
    focus: int,
    fiducialless: int,
    trackingMethod: int,
    TwoSurfaces: int,
    TargetNumberOfBeads: int,
    LocalAlignments: int,
    THICKNESS: int,
    # end user facing adoc params
    file_share: str,
    input_dir: str,
    x_file_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    token: Optional[str] = None,
    x_no_api: bool = False,
    x_keep_workdir: bool = False,
    adoc_template: str = "plastic_brt",
):
    utils.notify_api_running(x_no_api, token, callback_url)

    # a single input_dir will have n tomograms
    input_dir_fp = utils.get_input_dir.submit(
        share_name=file_share, input_dir=input_dir
    )
    # input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    input_fps = utils.list_files.submit(
        input_dir=input_dir_fp, exts=["MRC", "ST", "mrc", "st"], single_file=x_file_name
    )

    fps = utils.gen_fps.submit(
        share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps
    )
    brt_outputs = utils.run_brt.map(
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

    tilt_movies = gen_tilt_movie.map(brt_outputs)
    tilt_movie_assets = copy_asset_gen_elt.map(
        file_path=fps, fp_to_cp=tilt_movies, asset_type=unmapped(AssetType.TILT_MOVIE)
    )

    mid_images = find_middle_image.map(tilt_movies)
    keyimg_assets = copy_asset_gen_elt.map(
        file_path=fps, fp_to_cp=mid_images, asset_type=unmapped(AssetType.KEY_IMAGE)
    )

    thumbs = gen_thumbs.map(mid_images)
    thumb_assets = copy_asset_gen_elt.map(
        file_path=fps, fp_to_cp=thumbs, asset_type=unmapped(AssetType.THUMBNAIL)
    )

    ave_mrcs = gen_ave_mrc.map(brt_output=brt_outputs)
    averagedVolume_assets = copy_asset_gen_elt.map(
        file_path=fps, fp_to_cp=ave_mrcs, asset_type=unmapped(AssetType.VOLUME)
    )

    recon_movies = gen_recon_movie.map(ave_mrc=ave_mrcs)
    recon_movie_assets = copy_asset_gen_elt.map(
        file_path=fps, fp_to_cp=recon_movies, asset_type=unmapped(AssetType.REC_MOVIE)
    )
    # Binned volume assets, for volslicer.
    bin_vol_mrcs = gen_ave_8_vol.map(ave_mrc=ave_mrcs)
    bin_vol_assets = copy_asset_gen_elt.map(
        file_path=fps,
        fp_to_cp=bin_vol_mrcs,
        asset_type=unmapped(AssetType.AVERAGED_VOLUME),
    )

    zarrs = gen_zarr.map(brt_output=brt_outputs)
    pyramid_assets = gen_ng_metadata.map(fp_in=fps, zarr=zarrs)

    # now we've done the computational work.
    # the relevant files have been put into the Assets dirs, but we need to inform the API
    # Generate a base "primary path" dict, and hang dicts onto this.
    # repeatedly pass asset in to add_asset func to add asset in question.
    # allow_failure in gen_fps because we want to include EVERY fp, not just OK ones
    # prim_fps = utils.gen_prim_fps.map(fp_in=fps, wait_for=[allow_failure(brt_outputs)])
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

    # Ref: https://github.com/PrefectHQ/prefect/blob/98d33187ecce032defb8ec7a263de32564e7f7f6/src/prefect/futures.py#L43
    callback_result = list()
    failed = 0
    total = len(prim_fps)
    for idx, (fp, cb) in enumerate(zip(fps.result(), callback_with_tilt_mov)):
        # Wait for the task to complete
        # It does not mean that future state will be a terminal state
        state = cb.wait()
        if state.is_completed():
            callback_result.append(cb.result())
        else:
            path = f"{state.state_details.flow_run_id}__{idx}"
            try:
                message = BRTConfig.local_storage.read_path(path)
                callback_result.append(fp.gen_prim_fp_elt(message.decode()))
            except ValueError:
                callback_result.append(fp.gen_prim_fp_elt("Something went wrong!"))
            failed += 1

    utils.send_callback_body.submit(
        x_no_api=x_no_api,
        token=token,
        callback_url=callback_url,
        files_elts=callback_result,
    )

    if failed < total:
        return Completed(message="At least one callback is correct!")
    return Failed(message="None of the files succeeded!")
