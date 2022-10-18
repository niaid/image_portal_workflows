import math
from typing import List
from pathlib import Path
from prefect import Flow, task, Parameter, unmapped
from prefect.run_configs import LocalRun
from prefect.tasks.control_flow import merge
from em_workflows.config import Config
from em_workflows.shell_task_echo import ShellTaskEcho
from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng

shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


@task
def gen_xfalign_comand(fp_in: Path, fp_out) -> str:
    """
    hardcoded
    xfalign -pa -1 -pr {WORKDIR}/Source.mrc {WORKDIR}/align.xf
    """
    cmd = f"{Config.xfalign_loc} -pa -1 -pr {fp_in} {fp_out} &> {fp_out.parent}/xfalign.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_xftoxg_comand(fp_in: Path, fp_out: Path) -> str:
    """
    hardcoded
    xftoxg -ro -mi 2 {WORKDIR}/align.xf {WORKDIR}/align.xg
    """
    cmd = (
        f"{Config.xftoxg_loc} -ro -mi 2 {fp_in} {fp_out} &> {fp_out.parent}/xftoxg.log"
    )
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_newstack_align_command(
    align_xg: Path, source_mrc: Path, align_mrc: Path
) -> str:
    """
    generates align.mrc
    newstack -x {WORKDIR}/align.xg {WORKDIR}/Source.mrc {WORKDIR}/Align.mrc
    """
    cmd = f"{Config.newstack_loc} -x {align_xg} {source_mrc} {align_mrc} &> {align_mrc.parent}/newstack_align.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_tif_mrc_command(input_dir: Path, fp_out: Path) -> str:
    """
    generates source.mrc
    uses all the tifs in dir
    # tif2mrc {DATAPATH}/*.tif {WORKDIR}/Source.mrc
    """
    cmd = f"{Config.tif2mrc_loc} {input_dir}/*.tif {fp_out} &> {fp_out.parent}/tif2mrc.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def create_stretch_file(tilt: str, fp_out: Path) -> None:
    """
    creates stretch.xf
    used to gen corrected.mrc
    file looks like:
    1 0 0 {TILT_PARAMETER} 0 0

    where TILT_PARAMETER is calculated as 1/cos({TILT_ANGLE}).
    Note that tilt angle is specified in degrees.
    """
    # math.cos expects radians, convert to degrees first.
    tilt_angle = 1 / math.cos(math.degrees(float(tilt)))
    utils.log(f"creating stretch file, tilt_angle: {tilt_angle}.")
    fp_out.touch()
    with open(fp_out.as_posix(), "w") as _file:
        _file.write(f"1 0 0 {tilt_angle} 0 0")


@task
def gen_newstack_corr_command(stretch_fp: Path, aligned_fp: Path, fp_out: Path) -> str:
    """
    generates corrected.mrc
    uses the stretch file from create_stretch_file()

    newstack -x {WORKDIR}/stretch.xf {WORKDIR}/aligned.mrc {WORKDIR}/corrected.mrc
    """
    cmd = f"{Config.newstack_loc} -x {stretch_fp} {aligned_fp} {fp_out} &> {fp_out.parent}/newstack_cor.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_newstack_norm_command(fp_in: Path, fp_out: Path) -> str:
    """
    generates basename.mrc
    MRC file that will be used for all subsequent operations:

    newstack -meansd 150,40 -mo 0 align.mrc|corrected.mrc {BASENAME}.mrc
    """
    cmd = f"{Config.newstack_loc} -meansd 150,40 -mo 0 {fp_in} {fp_out} &> {fp_out.parent}/newstack_norm.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_newstack_mid_mrc_command(fps: List[Path], fp_in: Path, fp_out: Path) -> str:
    """
    generates mid.mrc
    newstack -secs {MIDZ}-{MIDZ} {WORKDIR}/{BASENAME}.mrc {WORKDIR}/mid.mrc
    """
    mid_z = int(len(fps) / 2)
    cmd = f"{Config.newstack_loc} -secs {mid_z} {fp_in} {fp_out} &> {fp_out.parent}/newstack_mid.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_keyimg_cmd(basename_mrc_fp: Path, fp_out: Path) -> str:
    """
    generates keyimg (large thumb)
    mrc2tif -j -C 0,255 {WORKDIR}/{BASENAME}.mrc {WORKDIR}/hedwig/keyimg_{BASENAME}.jpg
    """
    cmd = f"{Config.mrc2tif_loc} -j -C 0,255 {basename_mrc_fp} {fp_out} &> {fp_out.parent}/mrc2tif.log"
    utils.log(f"Created keyimg {cmd}")
    return cmd


@task
def gen_keyimg_small_cmd(keyimg_fp: Path, keyimg_sm_fp) -> str:
    """
    convert -size 300x300 {WORKDIR}/hedwig/keyimg_{BASENAME}.jpg \
            -resize 300x300 -sharpen 2 -quality 70 {WORKDIR}/hedwig/keyimg_{BASENAME}_s.jpg
    """
    cmd = f"{Config.convert_loc} -size 300x300 {keyimg_fp} -resize 300x300 -sharpen 2 -quality 70 {keyimg_sm_fp} &> {keyimg_sm_fp.parent}/convert.log"
    utils.log(f"Created {cmd}")
    return cmd


@task
def gen_basename(fps: List[Path]) -> Path:
    """
    For BASENAME, name of the first found tiff file in the stack,
    no extension, trailing digits, dashes, and underscores trimmed.
    TODO - Forrest asks: Can we use the dir name as the basename
    eg a sample will look like
    sample_dir/thing_a, sample_dir/thing_b
    use thing_a and thing_b as basenames
    TODO
    """
    return Path(fps[0].stem)


with Flow(
    "sem_tomo",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=Config.SLURM_EXECUTOR,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    tilt_angle = Parameter("tilt_angle", default=None)()

    # dir to read from.
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    input_dir_fps = utils.list_dirs(input_dir_fp=input_dir_fp)
    input_dir_fps_escaped = utils.sanitize_file_names(fps=input_dir_fps)

    # input files to work on.
    tif_fps = utils.list_files.map(
        input_dir=input_dir_fps, exts=unmapped(["TIFF", "tiff", "TIF", "tif"])
    )
    # check there's something relevent in the input dir (raises exp)
    utils.check_inputs_ok.map(tif_fps)

    # dir in which to do work in
    work_dirs = utils.set_up_work_env.map(input_dir_fps)
    # outputs dir, to move results to.
    assets_dirs = utils.make_assets_dir.map(input_dir=input_dir_fps)
    # escape bad chars in file names
    # only used for first step - gen_output_fp will translate to underscores
    # tif_fps_escaped= utils.sanitize_file_names.map(tif_fps)

    # gen source.mrc file
    source_mrc_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("source")),
        output_ext=unmapped(".mrc"),
    )
    source_mrc_commands = gen_tif_mrc_command.map(
        input_dir=input_dir_fps_escaped, fp_out=source_mrc_fps
    )
    source_mrcs = shell_task.map(
        command=source_mrc_commands, to_echo=unmapped(source_mrc_commands)
    )

    # using source.mrc gen align.xf
    xf_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("align")),
        output_ext=unmapped(".xf"),
    )
    xf_commands = gen_xfalign_comand.map(fp_in=source_mrc_fps, fp_out=xf_fps)
    xf_aligns = shell_task.map(
        command=xf_commands,
        to_echo=unmapped(xf_commands),
        upstream_tasks=[source_mrcs],
    )

    # using align.xf create align.xg
    xg_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("align")),
        output_ext=unmapped(".xg"),
    )
    xg_commands = gen_xftoxg_comand.map(fp_in=xf_fps, fp_out=xg_fps)
    xgs = shell_task.map(
        command=xg_commands,
        to_echo=unmapped(xg_commands),
        upstream_tasks=[xf_aligns],
    )

    # using align.xg create align.mrc
    mrc_align_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("align")),
        output_ext=unmapped(".mrc"),
    )
    mrc_align_commands = gen_newstack_align_command.map(
        align_xg=xg_fps, source_mrc=source_mrc_fps, align_mrc=mrc_align_fps
    )
    mrc_aligns = shell_task.map(
        command=mrc_align_commands,
        to_echo=unmapped(mrc_align_commands),
        upstream_tasks=[xgs],
    )

    # create stretch file using tilt_parameter
    # this only gets exec if tilt_parameter is not None
    # if tilt_angle is spec'd, copy corrected.mrc to Assets
    # else copy align_mrc file
    stretch_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("stretch")),
        output_ext=unmapped(".xf"),
    )
    stretchs = create_stretch_file.map(tilt=unmapped(tilt_angle), fp_out=stretch_fps)

    corrected_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("corrected")),
        output_ext=unmapped(".mrc"),
    )
    newstack_cor_cmds = gen_newstack_corr_command.map(
        stretch_fp=stretch_fps, aligned_fp=mrc_align_fps, fp_out=corrected_fps
    )
    newstack_cors = shell_task.map(
        command=newstack_cor_cmds,
        to_echo=unmapped(newstack_cor_cmds),
        upstream_tasks=[mrc_aligns, stretchs],
    )

    # the normalized step uses corrected_fp if tilt is specified
    # else it uses align_fp
    norm_input_fps = merge(corrected_fps, mrc_align_fps)

    # newstack normalized,
    basenames = gen_basename.map(fps=tif_fps)
    norm_mrc_fps = utils.gen_output_fp.map(
        working_dir=work_dirs, input_fp=basenames, output_ext=unmapped(".mrc")
    )
    newstack_norm_cmds = gen_newstack_norm_command.map(
        fp_in=norm_input_fps, fp_out=norm_mrc_fps
    )
    newstack_norms = shell_task.map(
        command=newstack_norm_cmds,
        to_echo=unmapped(newstack_norm_cmds),
        upstream_tasks=[newstack_cors],
    )

    # newstack mid, gen mid.mrc
    mid_mrc_fps = utils.gen_output_fp.map(
        working_dir=work_dirs,
        input_fp=unmapped(Path("mid")),
        output_ext=unmapped(".mrc"),
    )
    newstack_mid_cmds = gen_newstack_mid_mrc_command.map(
        fps=tif_fps, fp_in=norm_mrc_fps, fp_out=mid_mrc_fps
    )
    mid_mrc = shell_task.map(
        command=newstack_mid_cmds,
        to_echo=unmapped(newstack_mid_cmds),
        upstream_tasks=[newstack_norms],
    )

    # generate keyimg
    keyimg_fps = utils.gen_output_fp.map(
        working_dir=work_dirs, input_fp=basenames, output_ext=unmapped("_keyimg.jpg")
    )
    keyimg_cmds = gen_keyimg_cmd.map(basename_mrc_fp=mid_mrc_fps, fp_out=keyimg_fps)
    keyimgs = shell_task.map(
        command=keyimg_cmds,
        to_echo=unmapped(keyimg_cmds),
        upstream_tasks=[mid_mrc],
    )

    # generate keyimg small (thumbnail)
    keyimg_sm_fps = utils.gen_output_fp.map(
        working_dir=work_dirs, input_fp=basenames, output_ext=unmapped("_keyimg_sm.jpg")
    )
    keyimg_sm_cmds = gen_keyimg_small_cmd.map(
        keyimg_fp=keyimg_fps, keyimg_sm_fp=keyimg_sm_fps
    )
    keyimg_sms = shell_task.map(
        command=keyimg_sm_cmds,
        to_echo=unmapped(keyimg_sm_cmds),
        upstream_tasks=[keyimgs],
    )

    # START PYRAMID GEN
    mrc2nifti_cmds = ng.gen_mrc2nifti_cmd.map(
        fp=norm_mrc_fps, upstream_tasks=[newstack_norms]
    )
    mrc2niftis = shell_task.map(command=mrc2nifti_cmds, to_echo=unmapped(norm_mrc_fps))

    ##
    ng_fps = ng.gen_pyramid_outdir.map(fp=norm_mrc_fps)
    pyramid_cmds = ng.gen_pyramid_cmd.map(
        fp=norm_mrc_fps, outdir=ng_fps, upstream_tasks=[mrc2niftis]
    )
    gen_pyramids = shell_task.map(command=pyramid_cmds, to_echo=unmapped(pyramid_cmds))
    ##

    ##
    min_max_fps = utils.gen_output_fp.map(
        input_fp=norm_mrc_fps, output_ext=unmapped("_min_max.json")
    )
    min_max_cmds = ng.gen_min_max_cmd.map(
        fp=norm_mrc_fps, out_fp=min_max_fps, upstream_tasks=[mrc2niftis]
    )
    min_maxs = shell_task.map(command=min_max_cmds, to_echo=unmapped(min_max_cmds))
    metadatas = ng.parse_min_max_file.map(fp=min_max_fps, upstream_tasks=[min_maxs])
    # END PYRAMID
    #

    # copy over the mrc file used to Assets dir, as might be useful.
    # Note, this is not reported to API!
    # setting newstack_norms as upstream isn't exactly correct, but
    # it's a merged value, and newstack_norms is first usage.
    utils.copy_to_assets_dir.map(
        fp=norm_input_fps,
        assets_dir=assets_dirs,
        upstream_tasks=[newstack_norms],
    )
    # generate base element
    callback_base_elts = utils.gen_callback_elt.map(input_fname=input_dir_fps)

    # thumnails (small thumbs)
    thumbnail_fps = utils.copy_to_assets_dir.map(
        fp=keyimg_sm_fps,
        assets_dir=assets_dirs,
        # prim_fp=norm_mrc_fps,
        upstream_tasks=[keyimg_sms],
    )
    callback_with_thumbs = utils.add_assets_entry.map(
        base_elt=callback_base_elts,
        path=thumbnail_fps,
        asset_type=unmapped("thumbnail"),
    )

    # keyimg
    keyimg_fp_assets = utils.copy_to_assets_dir.map(
        fp=keyimg_fps,
        assets_dir=assets_dirs,
        # prim_fp=norm_mrc_fps,
        upstream_tasks=[keyimgs],
    )
    callback_with_keyimgs = utils.add_assets_entry.map(
        base_elt=callback_with_thumbs,
        path=keyimg_fp_assets,
        asset_type=unmapped("keyImage"),
    )

    # neuroglancerPrecomputed
    ng_asset_fps = utils.copy_to_assets_dir.map(
        fp=ng_fps,
        assets_dir=assets_dirs,
        # prim_fp=norm_mrc_fps,
        upstream_tasks=[gen_pyramids, metadatas],
    )
    callback_with_neuroglancer = utils.add_assets_entry.map(
        base_elt=callback_with_keyimgs,
        path=ng_asset_fps,
        asset_type=unmapped("neuroglancerPrecomputed"),
        metadata=metadatas,
    )

    cb = utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=callback_with_neuroglancer
    )

    cp = utils.cp_logs_to_assets.map(
        working_dir=work_dirs, assets_dir=assets_dirs, upstream_tasks=[unmapped(cb)]
    )
    utils.cleanup_workdir.map(wd=work_dirs, upstream_tasks=[unmapped(cp)])
