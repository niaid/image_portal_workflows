from typing import Optional, List
from pathlib import Path
from prefect.engine import signals
from prefect import Flow, task, Parameter, unmapped
from prefect.triggers import always_run
from prefect.tasks.docker.containers import (
    CreateContainer,
    StartContainer,
    WaitOnContainer,
    GetContainerLogs,
)


from image_portal_workflows.config import Config


class Job:
    def __init__(self, input_dir):
        self.output_dir = Path("/io/")
        self.input_dir = Path(input_dir)


class Container_Dm2mrc(CreateContainer):
    def run(self, input_dir: str, fp: Path):
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir + ":/io"]},
            command=[
                Config.dm2mrc_loc,
                f"/io/{fp.stem}.dm4",
                f"/io/{fp.stem}.mrc",
            ],
        )


class Container_gm_convert(CreateContainer):
    gm = "/usr/bin/gm"
    cmd = "gm convert"
    sharpen = "2"
    # docker run
    # -v $(pwd)/test/input_files/:/io
    # graphicsmagick gm convert
    # -size 300x300 "/io/20210525_1416_A000_G000.jpeg"
    # -resize 300x300 -sharpen 2 -quality 70 "/io/20210525_1416_A000_G000_sm.jpeg"

    def run(self, input_dir: str, fp: Path, size: str):
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
                f"/io/{fp.stem}.jpeg",
                "-resize",
                scaler,
                "-sharpen",
                "2",
                "-quality",
                "70",
                f"/io/{fp.stem}_{size}.jpeg",
            ],
        )


class Container_Mrc2tif(CreateContainer):
    def run(self, input_dir: str, fp: Path):
        return super().run(
            image_name="imod",
            volumes=[f"{input_dir}:/io"],
            host_config={"binds": [input_dir + ":/io"]},
            command=[
                Config.mrc2tif_loc,
                "-j",
                f"/io/{fp.stem}.mrc",
                f"/io/{fp.stem}.jpeg",
            ],
        )


create_mrc = Container_Dm2mrc()
create_jpeg = Container_Mrc2tif()
create_thumb = Container_gm_convert()

start = StartContainer()
wait = WaitOnContainer()
logs = GetContainerLogs(trigger=always_run)


@task
def get_files(job: Job, ext: str) -> Optional[List[Path]]:
    _files = list(job.input_dir.glob(f"**/*.{ext}"))
    if not _files:
        raise ValueError(f"{job.input_dir} contains no files with extension {ext}")
    return _files


@task
def init_job(input_dir: Path) -> Job:
    return Job(input_dir=input_dir)


with Flow("dm_to_jpeg") as flow:
    input_dir = Parameter("input_dir")
    job = init_job(input_dir=input_dir)
    dm4_fps = get_files(job, "dm4")

    dm_container_ids = create_mrc.map(input_dir=unmapped(input_dir), fp=dm4_fps)
    dm_container_starts = start.map(dm_container_ids)
    status_code = wait.map(dm_container_ids)

    jpeg_container_ids = create_jpeg.map(
        input_dir=unmapped(input_dir), fp=dm4_fps, upstream_tasks=[status_code]
    )
    jpeg_container_starts = start.map(jpeg_container_ids)
    jpeg_status_codes = wait.map(jpeg_container_ids)

    thumb_container_ids = create_thumb.map(
        input_dir=unmapped(input_dir), fp=dm4_fps, size=unmapped("sm"), upstream_tasks=[status_code]
    )
    thumb_container_starts = start.map(thumb_container_ids)
    thumb_status_codes = wait.map(thumb_container_ids)

    thumb_container_ids_lg = create_thumb.map(
        input_dir=unmapped(input_dir), fp=dm4_fps, size=unmapped("lg"), upstream_tasks=[status_code]
    )
    thumb_container_starts = start.map(thumb_container_ids_lg)
    thumb_status_codes = wait.map(thumb_container_ids_lg)
#    logs = logs(_id, upstream_tasks=[status_code])
