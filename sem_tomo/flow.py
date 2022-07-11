import math
import prefect
from pathlib import Path
from prefect import Flow, task, Parameter, unmapped, context
from image_portal_workflows.config import Config
from image_portal_workflows.shell_task_echo import ShellTaskEcho
from image_portal_workflows.utils import utils

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
def gen_newstack_command(fp_in: Path, fp_out: Path) -> str:
    """
    hardcoded
    newstack -x {WORKDIR}/align.xg {WORKDIR}/Source.mrc {WORKDIR}/Align.mrc
    """
    cmd = f"{Config.xftoxg_loc} -ro -mi 2 {fp_in} {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd

@task
def gen_tif_mrc_command(input_dir: Path, fp_out: Path) -> str:
    """
    generates the command that creates the Source file.
    # tif2mrc {DATAPATH}/*.tif {WORKDIR}/Source.mrc
    """
    cmd = f"{Config.tif2mrc_loc} {input_dir}/*.tif {fp_out}"
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd

@task
def create_stretch_file(tilt:str, fp_out: Path) -> None:
    """
    creates a file that's used to gen corrected.mrc
    file looks like:
    1 0 0 {TILT_PARAMETER} 0 0

    where TILT_PARAMETER is calculated as 1/cos({TILT_ANGLE}).
    Note that tilt angle is specified in degrees.
    """
    # math.cos expects radians, convert to degrees first.
    tilt_parameter = 1/math.cos(math.degrees(float(tilt)))
    fp_out.touch()
    with open(fp_out.as_posix(), 'w') as _file:
        _file.write(f"1 0 0 {tilt_parameter} 0 0")

@task
def gen_newstack_corr_command(stretch_fp: Path, aligned_fp: Path, fp_out: Path) -> str:
    """
    generates corrected.mrc using the stretch file from create_stretch_file()
    newstack -x {WORKDIR}/stretch.xf {WORKDIR}/aligned.mrc {WORKDIR}/corrected.mrc
    """
    cmd = f"{Config.newstack_loc} -x {stretch_fp} {aligned_fp} {fp_out}" 
    prefect.context.get("logger").info(f"Created {cmd}")
    return cmd

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
    tilt_parameter = Parameter("tilt_parameter")()

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
        input_fp=path("source"),
        output_ext=".mrc",
    )
    source_mrc_command = gen_tif_mrc_command(input_dir=input_dir_fp, fp_out=source_mrc_fp)
    source_mrc = shell_task(
        command=source_mrc_command, to_echo="running source.mrc"
    )

    # using source.mrc gen align.xf
    xf_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=path("align"),
        output_ext=".xf",
    )
    xf_command = gen_xfalign_comand(fp_in=source_mrc_fp, fp_out=xf_fp)
    xf_align = shell_task(command=xf_command, to_echo="running xf_align", upstream_tasks=[source_mrc])

    # using align.xf create align.xg
    xg_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=path("align"),
        output_ext=".xg",
    )
    xg_command = gen_xftoxg_comand(fp_in=xf_fp, fp_out=xg_fp)
    xg = shell_task(command=xg_command, to_echo="running xftoxg", upstream_tasks=[xf_align])

    # using align.xg create align.mrc
    mrc_fp = utils.gen_output_fp(
        working_dir=work_dir,
        input_fp=path("align"),
        output_ext=".mrc",
    )
    mrc_align_command = gen_newstack_command(fp_in=xg_fp, fp_out=mrc_fp)
    xg = shell_task(command=xg_command, to_echo="running newstack align", upstream_tasks=[xg])

    # create stretch file using tilt_parameter
    # this only gets exec if tilt_parameter is not None
    stretch_fp = utils.gen_output_fp(working_dir=work_dir,
            input_fp=Path("stretch"), output_ext=".xf")
    stretch = create_stretch_file(tilt=tilt_parameter, fp_out=stretch_fp)

    # this only gets exec if tilt_parameter
#    corrected_fp = utils.gen_output_fp(working_dir=work_dir,input_fp=Path("corrected"), output_ext=".mrc")
#    gen_newstack_corr_command(stretch_fp=stretch_fp, aligned_fp=mrc_fp, fp_out=corrected_fp)

    # newstack normalized, 
    #basename = gen_basename(fps=tif_fps)
    #norm_mrc_fp = utils.gen_output_fp()
    #newstack_norm_cmd = 

