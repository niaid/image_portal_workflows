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
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir + ":/io"]},
            command=[
                Config.dm2mrc_loc,
                f"/io/{fp.stem}.dm4",
                output_fp.as_posix(),
            ],
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
        logger.info(f"trying to convert {fp} to {output_fp}")
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
                fp.as_posix(),
                "-resize",
                scaler,
                "-sharpen",
                "2",
                "-quality",
                "70",
                output_fp.as_posix(),
            ],
        )


class Container_Mrc2tif(CreateContainer):
    def run(self, input_dir: str, fp: Path, output_fp: Path):
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir + ":/io"]},
            command=[
                Config.mrc2tif_loc,
                "-j",
                f"/io/{fp.stem}.mrc",
                output_fp.as_posix(),
            ],
        )


create_mrc = Container_Dm2mrc()
create_jpeg = Container_Mrc2tif()
create_thumb = Container_gm_convert()

start = StartContainer()
wait = WaitOnContainer()
logs = GetContainerLogs(trigger=always_run)


@task
def list_files(job: Job, ext: str) -> Optional[List[Path]]:
    _files = list(job.input_dir.glob(f"**/*.{ext}"))
    if not _files:
        raise ValueError(f"{job.input_dir} contains no files with extension {ext}")
    return _files


@task
def init_job(input_dir: Path) -> Job:
    return Job(input_dir=input_dir)


@task
def gen_output_fp(input_fp: Path, output_ext) -> Path:
    return Path(f"/io/{input_fp.stem}{output_ext}")


with Flow("dm_to_jpeg") as flow:
    input_dir = Parameter("input_dir")
    job = init_job(input_dir=input_dir)
    dm4_fps = list_files(job, "dm4")

    # dm* to mrc conversion
    mrc_locs = gen_output_fp.map(input_fp=dm4_fps, output_ext=unmapped(".mrc"))
    mrc_ids = create_mrc.map(
        input_dir=unmapped(input_dir), fp=dm4_fps, output_fp=mrc_locs
    )
    mrc_starts = start.map(mrc_ids)
    mrc_statuses = wait.map(mrc_ids)

    # mrc to jpeg conversion
    jpeg_locs = gen_output_fp.map(input_fp=mrc_locs, output_ext=unmapped(".jpeg"))
    jpeg_container_ids = create_jpeg.map(
        input_dir=unmapped(input_dir),
        fp=dm4_fps,
        output_fp=jpeg_locs,
        upstream_tasks=[mrc_statuses],
    )
    jpeg_container_starts = start.map(jpeg_container_ids)
    jpeg_status_codes = wait.map(jpeg_container_ids)

#    small_thumb_locs = gen_output_fp.map(
#        input_fp=jpeg_locs, output_ext=unmapped("_SM.jpeg")
#    )
#    thumb_container_ids_sm = create_thumb.map(
#        input_dir=unmapped(input_dir),
#        fp=jpeg_locs,
#        output_fp=small_thumb_locs,
#        size=unmapped("sm"),
#        upstream_tasks=[jpeg_status_codes],
#    )
#    thumb_container_starts_sm = start.map(thumb_container_ids_sm)
#    thumb_status_codes_sm = wait.map(thumb_container_ids_sm)
#
#    large_thumb_locs = gen_output_fp.map(
#        input_fp=jpeg_locs, output_ext=unmapped("_LG.jpeg")
#    )
#    thumb_container_ids_lg = create_thumb.map(
#        input_dir=unmapped(input_dir),
#        fp=jpeg_locs,
#        output_fp=large_thumb_locs,
#        size=unmapped("lg"),
#        upstream_tasks=[jpeg_status_codes],
#    )
#    thumb_container_starts_lg = start.map(thumb_container_ids_lg)
#    thumb_status_codes_lg = wait.map(thumb_container_ids_lg)
#    logs = logs(_id, upstream_tasks=[status_code])
