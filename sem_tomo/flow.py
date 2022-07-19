import math
from typing import List
import prefect
from pathlib import Path
from prefect import Flow, task, Parameter, case
from prefect.engine import signals
from prefect.tasks.control_flow import merge
from image_portal_workflows.config import Config
from image_portal_workflows.shell_task_echo import ShellTaskEcho
from image_portal_workflows.utils import utils
from image_portal_workflows.utils import neuroglancer as ng

shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


@task
def gen_xfalign_comand(fp_in: Path, fp_out) -> str:
    """
    hardcoded
    xfalign -pa -1 -pr {WORKDIR}/Source.mrc {WORKDIR}/align.xf
    """
    cmd = f"{Config.xfalign_loc} -pa -1 -pr {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_xftoxg_comand(fp_in: Path, fp_out: Path) -> str:
    """
    hardcoded
    xftoxg -ro -mi 2 {WORKDIR}/align.xf {WORKDIR}/align.xg
    """
    cmd = f"{Config.xftoxg_loc} -ro -mi 2 {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_newstack_align_command(
    align_xg: Path, source_mrc: Path, align_mrc: Path
) -> str:
    """
    generates align.mrc
    newstack -x {WORKDIR}/align.xg {WORKDIR}/Source.mrc {WORKDIR}/Align.mrc
    """
    cmd = f"{Config.newstack_loc} -x {align_xg} {source_mrc} {align_mrc}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_tif_mrc_command(input_dir: Path, fp_out: Path) -> str:
    """
    generates source.mrc
    uses all the tifs in dir
    # tif2mrc {DATAPATH}/*.tif {WORKDIR}/Source.mrc
    """
    cmd = f"{Config.tif2mrc_loc} {input_dir}/*.tif {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
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
    prefect.context.get("logger").info(f"creating stretch file.")
    tilt_parameter = 1 / math.cos(math.degrees(float(tilt)))
    fp_out.touch()
    with open(fp_out.as_posix(), "w") as _file:
        _file.write(f"1 0 0 {tilt_parameter} 0 0")


@task
def gen_newstack_corr_command(stretch_fp: Path, aligned_fp: Path, fp_out: Path) -> str:
    """
    generates corrected.mrc
    uses the stretch file from create_stretch_file()

    newstack -x {WORKDIR}/stretch.xf {WORKDIR}/aligned.mrc {WORKDIR}/corrected.mrc
    """
    cmd = f"{Config.newstack_loc} -x {stretch_fp} {aligned_fp} {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_newstack_norm_command(fp_in: Path, fp_out: Path) -> str:
    """
    generates basename.mrc
    MRC file that will be used for all subsequent operations:

    newstack -meansd 150,40 -mo 0 align.mrc|corrected.mrc {BASENAME}.mrc
    """
    cmd = f"{Config.newstack_loc} -meansd 150,40 -mo 0 {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_newstack_mid_mrc_command(fps: List[Path], fp_in: Path, fp_out: Path) -> str:
    """
    generates mid.mrc
    newstack -secs {MIDZ}-{MIDZ} {WORKDIR}/{BASENAME}.mrc {WORKDIR}/mid.mrc
    """
    mid_z = int(len(fps) / 2)
    cmd = f"{Config.newstack_loc} -secs {mid_z} {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_keyimg_cmd(basename_mrc_fp: Path, fp_out: Path) -> str:
    """
    generates keyimg (large thumb)
    mrc2tif -j -C 0,255 {WORKDIR}/{BASENAME}.mrc {WORKDIR}/hedwig/keyimg_{BASENAME}.jpg
    """
    cmd = f"{Config.mrc2tif_loc} -j -C 0,255 {basename_mrc_fp} {fp_out}"
    prefect.context.get("logger").info(f"Created keyimg {cmd}")
    return cmd


@task
def gen_keyimg_small_cmd(keyimg_fp: Path, keyimg_sm_fp) -> str:
    """
    convert -size 300x300 {WORKDIR}/hedwig/keyimg_{BASENAME}.jpg \
            -resize 300x300 -sharpen 2 -quality 70 {WORKDIR}/hedwig/keyimg_{BASENAME}_s.jpg
    """
    cmd = f"{Config.convert_loc} -size 300x300 {keyimg_fp} \
            -resize 300x300 -sharpen 2 -quality 70 {keyimg_sm_fp}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd


@task
def gen_basename(fps: List[Path]) -> Path:
    """
    For BASENAME, name of the first found tiff file in the stack,
    no extension, trailing digits, dashes, and underscores trimmed.
    """
    return Path(fps[0].stem)


@task
def check_tilt(s):
    """
    janky.
    function to try to cast a str to a float. This is to fit with prefect's
    https://docs.prefect.io/core/idioms/conditional.html conditional flow.
    """
    if s is None:
        return False
    try:
        float(s)
        return True
    except ValueError as ve:
        raise signals.FAIL(f"Tilt param expects a float.\n {ve}")
    except Exception as oe:
        raise signals.FAIL(f"This should not happen :-/ \n {oe}")


with Flow(
    "sem_tomo",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=Config.SLURM_EXECUTOR,
) as flow:
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    sample_id = Parameter("sample_id")()
    tilt_parameter = Parameter("tilt_parameter", default=None)()

    # dir to read from.
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)

    # dir in which to do work in
    work_dir = utils.make_work_dir()

    # outputs dir, to move results to.
    assets_dir = utils.make_assets_dir(input_dir=input_dir_fp)

    # input files to work on.
    tif_fps = utils.list_files(input_dir_fp, ["tif"], single_file=file_name)
    # check there's something relevent in the input dir (raises exp)
    utils.check_inputs_ok(tif_fps)

    # gen source.mrc file
    source_mrc_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=Path("source"),
        output_ext=".mrc",
    )
    source_mrc_command = gen_tif_mrc_command(
        input_dir=input_dir_fp, fp_out=source_mrc_fp
    )
    source_mrc = shell_task(command=source_mrc_command, to_echo="running source.mrc")

    # using source.mrc gen align.xf
    xf_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=Path("align"),
        output_ext=".xf",
    )
    xf_command = gen_xfalign_comand(fp_in=source_mrc_fp, fp_out=xf_fp)
    xf_align = shell_task(
        command=xf_command, to_echo="running xf_align", upstream_tasks=[source_mrc]
    )

    # using align.xf create align.xg
    xg_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=Path("align"),
        output_ext=".xg",
    )
    xg_command = gen_xftoxg_comand(fp_in=xf_fp, fp_out=xg_fp)
    xg = shell_task(
        command=xg_command, to_echo="running xftoxg", upstream_tasks=[xf_align]
    )

    # using align.xg create align.mrc
    mrc_align_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=Path("align"),
        output_ext=".mrc",
    )
    mrc_align_command = gen_newstack_align_command(
        align_xg=xg_fp, source_mrc=source_mrc_fp, align_mrc=mrc_align_fp
    )
    mrc_align = shell_task(
        command=mrc_align_command, to_echo="running newstack align", upstream_tasks=[xg]
    )

    use_tilt = check_tilt(tilt_parameter)
    with case(use_tilt, True):
        # create stretch file using tilt_parameter
        # this only gets exec if tilt_parameter is not None
        stretch_fp = utils.gen_output_fp(
            working_dir=work_dir, input_fp=Path("stretch"), output_ext=".xf"
        )
        stretch = create_stretch_file(tilt=tilt_parameter, fp_out=stretch_fp)

        corrected_fp = utils.gen_output_fp(
            working_dir=work_dir, input_fp=Path("corrected"), output_ext=".mrc"
        )
        newstack_cor_cmd = gen_newstack_corr_command(
            stretch_fp=stretch_fp, aligned_fp=mrc_align_fp, fp_out=corrected_fp
        )
        newstack_cor = shell_task(
            command=newstack_cor_cmd,
            to_echo="running newstack corrected",
            upstream_tasks=[mrc_align, stretch],
        )
    with case(use_tilt, False):
        # again, see https://docs.prefect.io/core/idioms/conditional.html
        align_fp = mrc_align_fp

    # the normalized step uses corrected_fp if tilt is specified
    # else it uses align_fp
    norm_input_fp = merge(corrected_fp, align_fp)

    # newstack normalized,
    basename = gen_basename(fps=tif_fps)
    norm_mrc_fp = utils.gen_output_fp(
        working_dir=work_dir, input_fp=basename, output_ext=".mrc"
    )
    newstack_norm_cmd = gen_newstack_norm_command(
        fp_in=norm_input_fp, fp_out=norm_mrc_fp
    )
    newstack_norm = shell_task(
        command=newstack_norm_cmd,
        to_echo="running newstack normalized",
        upstream_tasks=[newstack_cor],
    )

    # newstack mid, gen mid.mrc
    mid_mrc_fp = utils.gen_output_fp(
        working_dir=work_dir, input_fp=Path("mid"), output_ext=".mrc"
    )
    newstack_mid_cmd = gen_newstack_mid_mrc_command(
        fps=tif_fps, fp_in=norm_mrc_fp, fp_out=mid_mrc_fp
    )
    mid_mrc = shell_task(
        command=newstack_mid_cmd,
        to_echo="running newstack mid",
        upstream_tasks=[newstack_norm],
    )

    # generate keyimg
    keyimg_fp = utils.gen_output_fp(
        working_dir=work_dir, input_fp=basename, output_ext="_keyimg.jpg"
    )
    keyimg_cmd = gen_keyimg_cmd(basename_mrc_fp=mid_mrc_fp, fp_out=keyimg_fp)
    keyimg = shell_task(
        command=keyimg_cmd, to_echo="running key image", upstream_tasks=[mid_mrc]
    )

    # generate keyimg small (thumbnail)
    keyimg_sm_fp = utils.gen_output_fp(
        working_dir=work_dir, input_fp=basename, output_ext="_keyimg_sm.jpg"
    )
    keyimg_sm_cmd = gen_keyimg_small_cmd(keyimg_fp=keyimg_fp, keyimg_sm_fp=keyimg_sm_fp)
    keyimg_sm = shell_task(
        command=keyimg_sm_cmd,
        to_echo="running key small image",
        upstream_tasks=[keyimg],
    )

    # START PYRAMID GEN
    mrc2nifti_cmd = ng.gen_mrc2nifti_cmd(fp=norm_mrc_fp, upstream_tasks=[newstack_norm])
    mrc2nifti = shell_task(command=mrc2nifti_cmd, to_echo="mrc2nifti")

    ##
    ng_fp = ng.gen_pyramid_outdir(fp=norm_mrc_fp)
    pyramid_cmd = ng.gen_pyramid_cmd(
        fp=norm_mrc_fp, outdir=ng_fp, upstream_tasks=[mrc2nifti]
    )
    gen_pyramid = shell_task(command=pyramid_cmd, to_echo="gen pyramid")
    ##

    ##
    min_max_fp = utils.gen_output_fp(input_fp=norm_mrc_fp, output_ext="_min_max.json")
    min_max_cmd = ng.gen_min_max_cmd(
        fp=norm_mrc_fp, out_fp=min_max_fp, upstream_tasks=[mrc2nifti]
    )
    min_max = shell_task(command=min_max_cmd, to_echo="Min max")
    metadata = ng.parse_min_max_file(fp=min_max_fp, upstream_tasks=[min_max])
    # END PYRAMID
