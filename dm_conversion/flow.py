from pathlib import Path
from prefect import Flow, task, Parameter, unmapped, context
from prefect.tasks.docker.containers import (
    CreateContainer,
    StartContainer,
    WaitOnContainer,
    GetContainerLogs,
)
from prefect.triggers import always_run
from prefect.engine import signals


from image_portal_workflows.config import Config
from image_portal_workflows.utils import utils

logger = context.get("logger")


class Container_Dm2mrc(CreateContainer):
    """ """

    def run(self, input_dir: Path, fp: Path, output_fp: Path):
        logger.info(f"Dm2mrc mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir.as_posix() + ":/io"]},
            command=[Config.dm2mrc_loc, f_in, f_out],
        )


class Container_gm_convert(CreateContainer):
    gm = "/usr/bin/gm"
    sharpen = "2"
    # docker run
    # -v $(pwd)/test/input_files/:/io
    # graphicsmagick gm convert
    # -size 300x300 "/io/20210525_1416_A000_G000.jpeg"
    # -resize 300x300 -sharpen 2 -quality 70 "/io/20210525_1416_A000_G000_sm.jpeg"

    def run(self, input_dir: Path, fp: Path, output_fp: Path, size: str):
        logger.info(f"gm_convert mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        if size == "sm":
            scaler = Config.size_sm
        elif size == "lg":
            scaler = Config.size_lg
        else:
            raise signals.FAIL(f"Thumbnail must be either sm or lg, not {size}")
        return super().run(
            image_name="graphicsmagick",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir.as_posix() + ":/io"]},
            command=[
                Container_gm_convert.gm,
                "convert",
                "-size",
                scaler,
                f_in,
                "-resize",
                scaler,
                "-sharpen",
                "2",
                "-quality",
                "70",
                f_out,
            ],
        )


class Container_Mrc2tif(CreateContainer):
    def run(self, input_dir: Path, fp: Path, output_fp: Path):
        logger.info(f"Mrc2tiff mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir.as_posix() + ":/io"]},
            command=[
                Config.mrc2tif_loc,
                "-j",
                f_in,
                f_out,
            ],
        )


create_mrc = Container_Dm2mrc()
create_jpeg = Container_Mrc2tif()
create_thumb = Container_gm_convert()

startDM = StartContainer()
startMRC = StartContainer()
startGM = StartContainer()
startGMlg = StartContainer()
waitDM = WaitOnContainer()
waitMRC = WaitOnContainer()
waitGMlg = WaitOnContainer()
waitGM = WaitOnContainer()


# get_logs = GetContainerLogs(trigger=always_run)


with Flow("dm_to_jpeg", state_handlers=[utils.notify_api_completion]) as flow:
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url")
    token = Parameter("token")
    sample_id = Parameter("sample_id")()
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    # job = init_job(input_dir=input_dir)
    dm_fps = utils.list_files(input_dir_fp, ["dm4", "dm3"])
    dm_fps = utils.run_single_file(input_fps=dm_fps, fp_to_check=file_name)
    output_dir_fp = utils.gen_output_dir(input_dir=input_dir)
    copied = utils.copy_inputs_to_outputs_dir(
        input_dir_fp=input_dir_fp, output_dir_fp=output_dir_fp
    )

    #    # dm* to mrc conversion
    mrc_locs = utils.gen_output_fname.map(input_fp=dm_fps, output_ext=unmapped(".mrc"))
    mrc_ids = create_mrc.map(
        input_dir=unmapped(output_dir_fp),
        fp=dm_fps,
        output_fp=mrc_locs,
    )
    mrc_starts = startDM.map(mrc_ids)
    mrc_statuses = waitDM.map(mrc_ids)

    # mrc to jpeg conversion
    jpeg_locs = utils.gen_output_fname.map(
        input_fp=mrc_locs, output_ext=unmapped(".jpeg")
    )
    jpeg_container_ids = create_jpeg.map(
        input_dir=unmapped(output_dir_fp),
        fp=mrc_locs,
        output_fp=jpeg_locs,
        upstream_tasks=[mrc_statuses, mrc_starts],
    )
    jpeg_container_starts = startMRC.map(jpeg_container_ids)
    jpeg_status_codes = waitMRC.map(jpeg_container_ids)

    # check input_dir for any tif / tiff files
    tiff_locs = utils.list_files(
        input_dir=output_dir_fp,
        exts=["tif", "tiff", "jpeg"],
        upstream_tasks=[jpeg_status_codes],
    )
    jpeg_locs = jpeg_locs + tiff_locs

    # size down jpegs for small thumbs
    small_thumb_locs = utils.gen_output_fname.map(
        input_fp=jpeg_locs, output_ext=unmapped("_SM.jpeg")
    )
    thumb_container_ids_sm = create_thumb.map(
        input_dir=unmapped(output_dir_fp),
        fp=jpeg_locs,
        output_fp=small_thumb_locs,
        size=unmapped("sm"),
    )
    thumb_container_starts_sm = startGM.map(thumb_container_ids_sm)
    thumb_status_codes_sm = waitGM.map(thumb_container_ids_sm)
    #    logs = get_logs.map(
    #        container_id=thumb_container_ids_sm, upstream_tasks=[thumb_status_codes_sm]
    #    )
    # utils.print_t.map(logs)

    # size dow jpegs for large thumbs
    large_thumb_locs = utils.gen_output_fname.map(
        input_fp=jpeg_locs, output_ext=unmapped("_LG.jpeg")
    )
    thumb_container_ids_lg = create_thumb.map(
        input_dir=unmapped(output_dir_fp),
        fp=jpeg_locs,
        output_fp=large_thumb_locs,
        size=unmapped("lg"),
    )
    thumb_container_starts_lg = startGMlg.map(thumb_container_ids_lg)
    thumb_status_codes_lg = waitGMlg.map(
        thumb_container_ids_lg, upstream_tasks=[thumb_container_starts_lg]
    )

    callback_files = utils.generate_callback_body(
        token,
        callback_url,
        input_dir,
        jpeg_locs,
        large_thumb_locs,
        small_thumb_locs,
        upstream_tasks=[thumb_container_starts_sm, thumb_container_starts_lg],
    )

    utils.clean_up_outputs_dir(assets_dir=output_dir_fp, to_keep=callback_files)

# logs = logs(_id, upstream_tasks=[status_code])
