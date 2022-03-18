#!/usr/bin/env python3
import glob
import os
from pathlib import Path
import shutil
import tempfile
import prefect
from jinja2 import Environment, FileSystemLoader
from prefect import task, Flow, Parameter
from prefect.engine import signals
from prefect.tasks.shell import ShellTask


from image_portal_workflows.utils import utils
from image_portal_workflows import config
from image_portal_workflows.config import Config

shell_task = ShellTask(helper_script="cd ~")


class BrtTask(ShellTask):
    def __init__(self, adoc_fp):
        self.command = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1"


@task
def make_work_dir() -> Path:
    return Path(tempfile.mkdtemp(dir=Config.tmp_dir))


@task
def prep_adoc(working_dir: Path, fname: Path) -> Path:
    """copy the template adoc file to the working_dir, and add vars"""
    repo_dir = os.path.join(os.path.dirname(__file__), "..")
    adoc_fp = f"{working_dir}/{fname.stem}.adoc_template"
    shutil.copyfile(f"{repo_dir}/templates/dirTemplate.adoc", adoc_fp)
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
def update_adoc(adoc_fp: Path) -> Path:
    """updates the adoc file with input params, TODO"""
    file_loader = FileSystemLoader(str(adoc_fp.parent))
    env = Environment(loader=file_loader)
    template = env.get_template(adoc_fp.name)
    vals = {
        "basename": adoc_fp.stem,
        "bead_size": 10,
        # "pixel_size": 4,
        # "newstack_bin_by_fact": 4,
        "light_beads": 0,
        "tilt_thickness": 256,
        # "runtime_bin_by_fact": 4,
        "montage": 0,
        "dataset_dir": str(adoc_fp.parent),
    }

    output = template.render(vals)
    adoc_loc = Path(f"{adoc_fp.parent}/{adoc_fp.stem}.adoc")
    with open(adoc_loc, "w") as _file:
        print(output, file=_file)
    logger = prefect.context.get("logger")
    logger.info(f"created {adoc_loc}")
    return adoc_loc


@task
def create_brt_command(adoc_fp: Path) -> str:
    logger = prefect.context.get("logger")
    cmd = f"{Config.brt_binary} -di {adoc_fp.as_posix()} -cp 8 -gpu 1"
    logger.info(cmd)
    return cmd


@task
def gen_dimension_command(adoc_fp: Path) -> str:
    logger = prefect.context.get("logger")
    ali_file = Path(f"{adoc_fp.parent}/{adoc_fp.stem}_ali.mrv")
    # formatted=($(header -s EBOV_VSV_1_preali.mrc))
    # dim=($(header -s EBOV_VSV_1_preali.mrc))
    cmd = f"header -s {ali_file}"
    logger.info(cmd)
    return cmd


@task
def check_brt_run_ok(adoc_fp: Path):
    """
    ensures the following files exist:
    BASENAME_rec.mrc - the source for the reconstruction movie
    BASENAME_full_rec.mrc - the source for the Neuroglancer pyramid
    BASENAME_ali.mrc
    """
    rec_file = Path(f"{adoc_fp.parent}/{adoc_fp.stem}_rec.mrc")
    full_rec_file = Path(f"{adoc_fp.parent}/{adoc_fp.stem}_full_rec.mrc")
    ali_file = Path(f"{adoc_fp.parent}/{adoc_fp.stem}_ali.mrc")
    for _file in [rec_file, full_rec_file, ali_file]:
        if not _file.exists():
            raise signals.FAIL(f"File {_file} does not exist. BRT run failure.")


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
    brt_command = create_brt_command(adoc_fp=updated_adoc)
    input_fp = prep_input_fp(fname=fname, working_dir=working_dir)
    brt = shell_task(command=brt_command, upstream_tasks=[input_fp])
    check_brt_run_ok(adoc_fp=updated_adoc, upstream_tasks=[brt])
    dimensions_cmd = gen_dimension_command(adoc_fp=updated_adoc, upstream_tasks=[brt])
    dimensions = shell_task(command=dimensions_cmd)
