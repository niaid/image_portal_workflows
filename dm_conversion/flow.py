from os import name
from typing import Optional, List
from pathlib import Path
from prefect.engine import signals
from prefect import Flow, task, Parameter, unmapped, context
from prefect.triggers import always_run
from prefect.tasks.docker.containers import (
    CreateContainer,
    StartContainer,
    WaitOnContainer,
    GetContainerLogs,
)


from image_portal_workflows.config import Config

logger = context.get("logger")


class Job:
    def __init__(self, input_dir):
        self.output_dir = Path("/io/")
        self.input_dir = Path(input_dir)


class Container_Dm2mrc(CreateContainer):
    def run(self, input_dir: str, fp: Path, output_fp: Path):
        logger.info(f"Dm2mrc mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir + ":/io"]},
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

    def run(self, input_dir: str, fp: Path, output_fp: Path, size: str):
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
            host_config={"binds": [input_dir + ":/io"]},
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
    def run(self, input_dir: str, fp: Path, output_fp: Path):
        logger.info(f"Mrc2tiff mounting {input_dir} to /io")
        f_in = f"/io/{fp.name}"
        f_out = f"/io/{output_fp.name}"
        logger.info(f"trying to convert {f_in} to {f_out}")
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir + ":/io"]},
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

start = StartContainer()
wait = WaitOnContainer()
logs = GetContainerLogs(trigger=always_run)


@task
def list_files(input_dir: str, ext: str) -> Optional[List[Path]]:
    input_path = Path(input_dir)
    _files = list(input_path.glob(f"**/*.{ext}"))
    _file_names = [Path(_file.name) for _file in _files]
    if not _files:
        raise ValueError(f"{job.input_dir} contains no files with extension {ext}")
    return _file_names


@task
def init_job(input_dir: Path) -> Job:
    return Job(input_dir=input_dir)


@task
def gen_output_fname(input_fp: Path, output_ext) -> Path:
    output_fp = Path(f"{input_fp.stem}{output_ext}")
    logger.info(f"input: {input_fp} output: {output_fp}")
    return output_fp


with Flow("dm_to_jpeg") as flow:
    input_dir = Parameter("input_dir")
    job = init_job(input_dir=input_dir)
    dm4_fps = list_files(input_dir, "dm4")

    #    mrc_id = create_mrc(
    #            input_dir=input_dir,
    #            fp=Path("/io/20210525_1416.dm4"),
    #            output_fp=Path("/io/20210525_1416.mrc"))
    #
    #    mrc_starts = start(mrc_id)
    #    mrc_statuses = wait(mrc_id)

    # dm* to mrc conversion
    mrc_locs = gen_output_fname.map(input_fp=dm4_fps, output_ext=unmapped(".mrc"))
    mrc_ids = create_mrc.map(
        input_dir=unmapped(input_dir), fp=dm4_fps, output_fp=mrc_locs
    )
    mrc_starts = start.map(mrc_ids)
    mrc_statuses = wait.map(mrc_ids)

    # mrc to jpeg conversion
    jpeg_locs = gen_output_fname.map(input_fp=mrc_locs, output_ext=unmapped(".jpeg"))
    jpeg_container_ids = create_jpeg.map(
        input_dir=unmapped(input_dir),
        fp=mrc_locs,
        output_fp=jpeg_locs,
        upstream_tasks=[mrc_statuses],
    )
    jpeg_container_starts = start.map(jpeg_container_ids)
    jpeg_status_codes = wait.map(jpeg_container_ids)

    # size down jpegs for small thumbs
    small_thumb_locs = gen_output_fname.map(
        input_fp=jpeg_locs, output_ext=unmapped("_SM.jpeg")
    )
    thumb_container_ids_sm = create_thumb.map(
        input_dir=unmapped(input_dir),
        fp=jpeg_locs,
        output_fp=small_thumb_locs,
        size=unmapped("sm"),
        upstream_tasks=[jpeg_status_codes],
    )
    thumb_container_starts_sm = start.map(thumb_container_ids_sm)
    thumb_status_codes_sm = wait.map(thumb_container_ids_sm)

#    # size dow jpegs for large thumbs
#    large_thumb_locs = gen_output_fname.map(
#        input_fp=jpeg_locs, output_ext=unmapped("_LG.jpeg")
#    )
#    thumb_container_ids_lg = create_thumb.map(
#        input_dir=unmapped(input_dir),
#        fp=jpeg_locs,
#        output_fp=large_thumb_locs,
#        size=unmapped("lg"),
#    )
#    thumb_container_starts_lg = start.map()
#    thumb_status_codes_lg = wait.map()
    # logs = logs(_id, upstream_tasks=[status_code])
