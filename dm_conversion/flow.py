import json
from pathlib import Path
from typing import List
import prefect
from prefect import Flow, task, Parameter, unmapped, context
from image_portal_workflows.shell_task_echo import ShellTaskEcho
from prefect.engine import signals


from image_portal_workflows.config import Config
from image_portal_workflows.utils import utils

logger = context.get("logger")
shell_task = ShellTaskEcho(log_stderr=True, return_all=True, stream_output=True)


# get_logs = GetContainerLogs(trigger=always_run)
@task
def create_dm2mrc_command(dm_fp: Path, output_fp: Path) -> str:
    cmd = f"{Config.dm2mrc_loc} {dm_fp} {output_fp}"
    prefect.context.get("logger").info(f"Generated cmd {cmd}")
    return cmd


@task
def create_jpeg_cmd(mrc_fp: Path, output_fp: Path) -> str:
    cmd = f"{Config.mrc2tif_loc} -j {mrc_fp} {output_fp}"
    prefect.context.get("logger").info(f"Generated cmd {cmd}")
    return cmd


@task
def create_gm_cmd(fp_in: Path, fp_out: Path, size: str) -> str:
    # gm convert
    # -size 300x300 "/io/20210525_1416_A000_G000.jpeg"
    # -resize 300x300 -sharpen 2 -quality 70 "/io/20210525_1416_A000_G000_sm.jpeg"
    if size == "sm":
        scaler = Config.size_sm
    elif size == "lg":
        scaler = Config.size_lg
    else:
        raise signals.FAIL(f"Thumbnail must be either sm or lg, not {size}")
    cmd = f"gm convert -size {scaler} {fp_in} -resize 300x300 -sharpen 2 -quality 70 {fp_out}"
    prefect.context.get("logger").info(f"Generated cmd {cmd}")
    return cmd


@task
def join_list(list1: List[Path], list2: List[Path]) -> List[Path]:
    return list1 + list2


@task
def dump_to_json(elt):
    print(json.dumps(elt))


with Flow(
    "dm_to_jpeg", state_handlers=[utils.notify_api_completion, utils.notify_api_running]
) as flow:
    """
    [dm4 and dm3 inputs] ---> [mrc intermediary files] ---> [jpeg outputs]    -->
                                                            [add jpeg inputs] -->
    --> ALL scaled into sizes "sm" and "lg"
    """
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    sample_id = Parameter("sample_id")()
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)

    # create a temp space to work
    temp_dir = utils.make_work_dir()

    # create an assets_dir (to copy required outputs into)
    assets_dir = utils.make_assets_dir(input_dir=input_dir_fp)
    # [dm4 and dm3 inputs]
    dm_fps = utils.list_files(input_dir_fp, ["dm4", "dm3"])

    # dm* to mrc conversion
    mrc_fps = utils.gen_output_fp.map(
        input_fp=dm_fps, working_dir=unmapped(temp_dir), output_ext=unmapped(".mrc")
    )
    dm2mrc_commands = create_dm2mrc_command.map(dm_fp=dm_fps, output_fp=mrc_fps)
    dm2mrcs = shell_task.map(
        command=dm2mrc_commands, to_echo=unmapped("running dm2mrc commands")
    )

    # mrc is intermed format, to jpeg conversion
    jpeg_fps = utils.gen_output_fp.map(input_fp=mrc_fps, output_ext=unmapped(".jpeg"))
    jpeg_commands = create_jpeg_cmd.map(mrc_fp=mrc_fps, output_fp=jpeg_fps)
    jpegs = shell_task.map(
        command=jpeg_commands,
        to_echo=unmapped("running jpeg commands"),
    )

    # need to scale the newly created jpegs
    jpeg_fps_sm_fps = utils.gen_output_fp.map(
        input_fp=jpeg_fps, upstream_tasks=[jpegs], output_ext=unmapped("_SM.jpeg")
    )
    jpeg_fps_sm_cmds = create_gm_cmd.map(
        fp_in=jpeg_fps, fp_out=jpeg_fps_sm_fps, size=unmapped("sm")
    )
    jpeg_fps_sms = shell_task.map(command=jpeg_fps_sm_cmds)

    # need to scale the newly created jpegs
    jpeg_fps_lg_fps = utils.gen_output_fp.map(
        input_fp=jpeg_fps, upstream_tasks=[jpegs], output_ext=unmapped("_LG.jpeg")
    )
    jpeg_fps_lg_cmds = create_gm_cmd.map(
        fp_in=jpeg_fps, fp_out=jpeg_fps_lg_fps, size=unmapped("lg")
    )
    jpeg_fps_lgs = shell_task.map(command=jpeg_fps_lg_cmds)
    # an input dir can also contain tif / tiff / jpeg or png files

    # other inputs need to be converted too. Hopefully there's no naming overlaps
    other_input_fps = utils.list_files(
        input_dir=input_dir_fp, exts=["tif", "tiff", "jpeg", "png"]
    )
    # files in input dir, need to use working_dir to path
    other_input_sm_fps = utils.gen_output_fp.map(
        input_fp=other_input_fps,
        output_ext=unmapped("_SM.jpeg"),
        working_dir=unmapped(temp_dir),
    )
    other_input_gm_sm_cmds = create_gm_cmd.map(
        fp_in=other_input_fps,
        fp_out=other_input_sm_fps,
        size=unmapped("sm"),
    )
    gm_cmd_in_dirs = shell_task.map(command=other_input_gm_sm_cmds)

    # large thumbs
    other_input_lg_fps = utils.gen_output_fp.map(
        input_fp=other_input_fps,
        output_ext=unmapped("_LG.jpeg"),
        working_dir=unmapped(temp_dir),
    )
    other_input_lg_cmds = create_gm_cmd.map(
        fp_in=other_input_fps, fp_out=other_input_lg_fps, size=unmapped("lg")
    )
    other_input_lgs = shell_task.map(command=other_input_lg_cmds)

    wrapper_elts = utils.generate_callback_files.map(input_fname=dm_fps)
    kts = utils.add_assets_entry.map(
        base_elt=wrapper_elts, path=jpeg_fps_sm_fps, asset_type=unmapped("keyThumbnail")
    )
    dump_to_json.map(wrapper_elts, upstream_tasks=[kts])

    # finished with other input conversion.

#    callback_files = utils.generate_callback_body(
#        token,
#        callback_url,
#        input_dir,
#        jpeg_locs,
#        large_thumb_locs,
#        small_thumb_locs,
#        upstream_tasks=[thumb_container_starts_sm, thumb_container_starts_lg],
#    )
#
#    utils.clean_up_outputs_dir(assets_dir=output_dir_fp, to_keep=callback_files)
