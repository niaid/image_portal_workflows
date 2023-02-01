from em_workflows.file_path import FilePath
import glob
import math
from typing import List, Dict
from prefect import Flow, task, Parameter, unmapped
from prefect.run_configs import LocalRun
from em_workflows.config import Config
from em_workflows.shell_task_echo import ShellTaskEcho
from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng

# shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


@task
def gen_xfalign_comand(fp_in: FilePath) -> None:
    """
    hardcoded
    xfalign -pa -1 -pr source.mrc align.xf
    """
    source_mrc = fp_in.gen_output_fp(out_fname="source.mrc")
    if not source_mrc.exists():
        utils.log(f"{source_mrc} does not exist")
    align_xf = fp_in.gen_output_fp(out_fname="align.xf")
    log_file = f"{source_mrc.parent}/xfalign.log"
    utils.log(f"xfalign log_file: {log_file}")
    cmd = [
        Config.xfalign_loc,
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
    hardcoded
    xftoxg -ro -mi 2 {WORKDIR}/align.xf {WORKDIR}/align.xg
    """
    align_xg = fp_in.gen_output_fp(out_fname="align.xg")
    align_xf = fp_in.gen_output_fp(out_fname="align.xf")
    log_file = f"{align_xg.parent}/xgalign.log"
    cmd = [
        Config.xftoxg_loc,
        "-ro",
        "-mi",
        "2",
        align_xf.as_posix(),
        align_xg.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_newstack_align(fp_in: FilePath) -> None:
    """
    generates align.mrc
    newstack -x {WORKDIR}/align.xg {WORKDIR}/Source.mrc {WORKDIR}/Align.mrc
    """
    align_mrc = fp_in.gen_output_fp(out_fname="align.mrc")
    align_xg = fp_in.gen_output_fp(out_fname="align.xg")
    source_mrc = fp_in.gen_output_fp(out_fname="source.mrc")

    log_file = f"{align_mrc.parent}/newstack_align.log"
    cmd = [
        Config.newstack_loc,
        "-x",
        align_xg.as_posix(),
        source_mrc.as_posix(),
        align_mrc.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def convert_tif_to_mrc(file_path: FilePath) -> int:
    """
    generates source.mrc
    assumes there's tifs in input dir
    uses all the tifs in dir
    # tif2mrc {DATAPATH}/*.tif {WORKDIR}/Source.mrc
    """
    output_fp = file_path.gen_output_fp(out_fname="source.mrc")
    log_file = f"{output_fp.parent}/tif2mrc.log"
    files = glob.glob(f"{file_path.fp_in.as_posix()}/*.tif")

    cmd = [Config.tif2mrc_loc]
    cmd.extend(files)
    cmd.append(output_fp.as_posix())
    utils.log(f"Created {cmd}")
    return_code = FilePath.run(cmd=cmd, log_file=log_file)
    return return_code


@task
def create_stretch_file(tilt: float, fp_in: FilePath) -> None:
    """
    creates stretch.xf
    used to gen corrected.mrc
    file looks like:
    1 0 0 {TILT_PARAMETER} 0 0

    where TILT_PARAMETER is calculated as 1/cos({TILT_ANGLE}).
    Note that tilt angle is input in degrees, however cos method expects radians
    """
    # math.cos expects radians, convert to degrees first.
    tilt_angle = 1 / math.cos(math.degrees(float(tilt)))
    utils.log(f"creating stretch file, tilt_angle: {tilt_angle}.")
    output_fp = fp_in.gen_output_fp(out_fname="stretch.xf")
    with open(output_fp.as_posix(), "w") as _file:
        _file.write(f"1 0 0 {tilt_angle} 0 0")


@task
def gen_newstack_corr_command(fp_in: FilePath) -> dict:
    """
    generates corrected.mrc
    uses the stretch file from create_stretch_file()

    newstack -x {WORKDIR}/stretch.xf {WORKDIR}/aligned.mrc {WORKDIR}/corrected.mrc
    """
    stretch_fp = fp_in.gen_output_fp(out_fname="stretch.xf")
    align_mrc = fp_in.gen_output_fp(out_fname="align.mrc")
    corrected_mrc = fp_in.gen_output_fp(out_fname="corrected.mrc")
    log_file = f"{stretch_fp.parent}/newstack_cor.log"
    cmd = [
        Config.newstack_loc,
        "-x",
        stretch_fp.as_posix(),
        align_mrc.as_posix(),
        corrected_mrc.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    assets_fp_corr_mrc = fp_in.copy_to_assets_dir(fp_to_cp=corrected_mrc)
    # we think that averagedVolume is not the best term here.
    # possibly use: alignedCorrectedVol
    return fp_in.gen_asset(asset_type="averagedVolume", asset_fp=assets_fp_corr_mrc)


@task
def gen_newstack_norm_command(fp_in: FilePath) -> None:
    """
    generates basename.mrc
    MRC file that will be used for all subsequent operations:

    newstack -meansd 150,40 -mo 0 corrected.mrc {BASENAME}.mrc
    """
    corrected_mrc = fp_in.gen_output_fp(out_fname="corrected.mrc")
    fp_out = fp_in.gen_output_fp(output_ext=".mrc")
    log_file = f"{fp_out.parent}/newstack_norm.log"
    cmd = [
        Config.newstack_loc,
        "-meansd",
        "150,40",
        "-mo",
        "0",
        corrected_mrc.as_posix(),
        fp_out.as_posix(),
    ]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    assets_fp_mrc = fp_in.copy_to_assets_dir(fp_to_cp=fp_out)


@task
def gen_newstack_mid_mrc_command(fp_in: FilePath) -> None:
    """
    generates mid.mrc
    newstack -secs {MIDZ}-{MIDZ} {WORKDIR}/{BASENAME}.mrc {WORKDIR}/mid.mrc
    """
    mid_mrc = fp_in.gen_output_fp(out_fname="mid.mrc")
    base_mrc = fp_in.gen_output_fp(output_ext=".mrc")
    utils.log(fp_in.fp_in.as_posix())
    tifs = glob.glob(f"{fp_in.fp_in.as_posix()}/*tif")
    utils.log(tifs)
    mid_z = str(int(len(tifs) / 2))
    log_file = f"{base_mrc.parent}/newstack_mid.log"
    cmd = [Config.newstack_loc, "-secs", mid_z, base_mrc.as_posix(), mid_mrc.as_posix()]
    utils.log(f"Created {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)


@task
def gen_keyimg(fp_in: FilePath) -> Dict:
    """
    generates keyimg (large thumb)
    mrc2tif -j -C 0,255 mid.mrc {WORKDIR}/keyimg_{BASENAME}.jpg
    """
    mid_mrc = fp_in.gen_output_fp(out_fname="mid.mrc")
    keyimg_fp = fp_in.gen_output_fp(out_fname="keyimg.jpg")
    log_file = f"{mid_mrc.parent}/mrc2tif.log"
    cmd = [
        Config.mrc2tif_loc,
        "-j",
        "-C",
        "0,255",
        mid_mrc.as_posix(),
        keyimg_fp.as_posix(),
    ]
    utils.log(f"Created keyimg {cmd}")
    FilePath.run(cmd=cmd, log_file=log_file)
    asset_fp = fp_in.copy_to_assets_dir(fp_to_cp=keyimg_fp)
    keyimg_asset = fp_in.gen_asset(asset_type="keyImage", asset_fp=asset_fp)
    return keyimg_asset


@task
def gen_keyimg_small(fp_in: FilePath) -> Dict:
    """
    convert -size 300x300 {WORKDIR}/hedwig/keyimg_{BASENAME}.jpg \
            -resize 300x300 -sharpen 2 -quality 70 {WORKDIR}/hedwig/keyimg_{BASENAME}_s.jpg
    """
    keyimg_fp = fp_in.gen_output_fp(out_fname="keyimg.jpg")
    keyimg_sm_fp = fp_in.gen_output_fp(out_fname="keyimg_sm.jpg")
    log_file = f"{keyimg_sm_fp.parent}/convert.log"
    cmd = [
        Config.convert_loc,
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
    keyimg_asset = fp_in.gen_asset(asset_type="thumbnail", asset_fp=asset_fp)
    return keyimg_asset


with Flow(
    "sem_tomo",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=Config.SLURM_EXECUTOR,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url", default=None)()
    token = Parameter("token", default=None)()
    tilt_angle = Parameter("tilt_angle", default=0)()
    no_api = Parameter("no_api", default=False)()

    # dir to read from.
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    # note FIBSEM is different to other flows in that it uses *directories*
    # to define stacks. Therefore, will have to list dirs to discover stacks
    # (rather than eg mrc files)
    input_dir_fps = utils.list_dirs(input_dir_fp=input_dir_fp)

    fps = utils.gen_fps(input_dir=input_dir_fp, fps_in=input_dir_fps)
    tif_to_mrc = convert_tif_to_mrc.map(fps)

    # using source.mrc gen align.xf
    align_xfs = gen_xfalign_comand.map(fp_in=fps, upstream_tasks=[tif_to_mrc])

    # using align.xf create align.xg
    gen_align_xgs = gen_align_xg.map(fp_in=fps, upstream_tasks=[align_xfs])

    # using align.xg create align.mrc
    align_mrcs = gen_newstack_align.map(fp_in=fps, upstream_tasks=[gen_align_xgs])

    # create stretch file using tilt_parameter
    stretchs = create_stretch_file.map(tilt=unmapped(tilt_angle), fp_in=fps)

    corrected_mrc_assets = gen_newstack_corr_command.map(
        fp_in=fps, upstream_tasks=[stretchs, align_mrcs]
    )

    corrected_movie_assets = utils.mrc_to_movie.map(
        file_path=fps,
        root=unmapped("corrected"),
        asset_type=unmapped("recMovie"),
        upstream_tasks=[corrected_mrc_assets],
    )

    base_mrcs = gen_newstack_norm_command.map(
        fp_in=fps, upstream_tasks=[corrected_mrc_assets]
    )

    # generate midpoint mrc file
    mid_mrcs = gen_newstack_mid_mrc_command.map(fp_in=fps, upstream_tasks=[base_mrcs])

    # large thumb
    keyimg_assets = gen_keyimg.map(fp_in=fps, upstream_tasks=[mid_mrcs])
    # small thumb
    thumb_assets = gen_keyimg_small.map(fp_in=fps, upstream_tasks=[keyimg_assets])

    # nifti file generation
    niftis = ng.gen_niftis.map(fp_in=fps, upstream_tasks=[base_mrcs])

    pyramid_assets = ng.gen_pyramids.map(fp_in=fps, upstream_tasks=[niftis])

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
        prim_fp=callback_with_pyramids, asset=corrected_mrc_assets
    )
    callback_with_corr_movies = utils.add_asset.map(
        prim_fp=callback_with_corr_mrcs, asset=corrected_movie_assets
    )

    # finally filter error states, and convert to JSON and send.
    filtered_callback = utils.filter_results(callback_with_corr_movies)
    cp_wd_to_assets = utils.copy_workdirs.map(
        fps, upstream_tasks=[callback_with_corr_movies]
    )
    cb = utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=filtered_callback
    )
