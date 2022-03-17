#!/usr/bin/env python3
import glob
import os
from pathlib import Path
import shutil
import tempfile
import prefect
from jinja2 import Environment, FileSystemLoader
from prefect import task, Flow, Parameter
from prefect.tasks.shell import ShellTask


from image_portal_workflows.utils import utils
from image_portal_workflows import config
from image_portal_workflows.config import Config

shell_task = ShellTask(helper_script="cd ~")

class BrtTask(ShellTask):
    def __init__(self, adoc_fp):
       self.command=f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1"

@task
def make_work_dir() -> Path:
    return Path(tempfile.mkdtemp(dir=Config.tmp_dir))


@task
def prep_adoc(working_dir: Path, fname: Path) -> Path:
    """copy the template adoc file to the working_dir, and add vars"""
    cwd = os.path.join(os.path.dirname(__file__), '..')
    adoc_fp = f"{working_dir}/{fname.stem}.adoc_template"
    shutil.copyfile(f"{cwd}/templates/dirTemplate.adoc", adoc_fp)
    return Path(adoc_fp)


@task
def list_input_dir(input_dir_fp: Path) -> Path:
    logger = prefect.context.get("logger")
    logger.info(f"trying to list {input_dir_fp}")

    mrc_files = glob.glob(f"{input_dir_fp}/*.mrc")
    # if len mrc_files == 0: raise?
    return Path(mrc_files[0])


@task
def prep_input_fp(fname: Path, working_dir: Path) -> Path:
    fp = shutil.copyfile(src=fname.as_posix(), dst=f"{working_dir}/{fname.name}")
    return Path(fp)


@task
def update_adoc(adoc_fp: Path) -> str:
    file_loader = FileSystemLoader(str(adoc_fp.parent))
    env = Environment(loader=file_loader)
    template = env.get_template(adoc_fp.name)
    vals = {
        "basename": adoc_fp.stem,
        "bead_size": 4,
        "montage": 0,
        "input_dir": str(adoc_fp.parent),
        "pixel_size": 4,
        "newstack_bin_by_fact": 4,
        "light_beads": 4,
        "tilt_thickness": 2,
    }

    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{adoc_fp.stem}.adoc")
    with open(adoc_loc, "w") as _file:
         print(output, file=_file)
    logger = prefect.context.get("logger")
    logger.info(f"created {adoc_loc}")
    cmd = f"{Config.brt_binary} -di {adoc_loc.as_posix()} -cp 8 -gpu 1"
    logger.info(cmd)
    # cmd = f"ls {adoc_loc.as_posix()}"
    return cmd


with Flow("brt_flow", executor=Config.SLURM_EXECUTOR) as flow:
    input_dir = Parameter("input_dir")
    callback_url = Parameter("callback_url")()
    token = Parameter("token")()
    sample_id = Parameter("sample_id")()
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)
    fname = list_input_dir(input_dir_fp=input_dir_fp)
    working_dir = make_work_dir()
    adoc_fp = prep_adoc(working_dir, fname)
    updated_adoc = update_adoc(adoc_fp)
    input_fp = prep_input_fp(fname=fname, working_dir=working_dir)
    c = shell_task(command=updated_adoc, upstream_tasks=[input_fp])
# # contents = shell_task(command=f"{Config.brt_binary} -di {adoc_fp} -cp 8 -gpu 1")
# print_l(str(adoc_fp))
