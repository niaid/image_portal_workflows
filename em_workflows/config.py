import os
from prefect.engine import signals
from pathlib import Path
from dask_jobqueue import SLURMCluster
from dotenv import load_dotenv
from prefect.executors import DaskExecutor
import prefect
import shutil

from em_workflows.constants import NFS_MOUNT

# loads .env file into os.environ
load_dotenv()


def SLURM_exec():
    """
    brings up a dynamically sized cluster.

    The job_script() for the given cluster generates:
    python -m distributed.cli.dask_worker tcp://<some-IP> --nthreads 15 --nworkers 4 --memory-limit 96.62GiB ...

    The processes determins number of dask workers, and nthreads = cores / processes
    The memory limit is also divided among the workers
    """
    home = os.environ["HOME"]
    cluster = SLURMCluster(
        name="dask-worker",
        cores=62,
        memory="430G",
        processes=4,
        death_timeout=121,
        local_directory=f"{home}/dask_tmp/",
        queue="gpu",
        walltime="24:00:00",
        job_extra_directives=["--gres=gpu:1"],
    )
    cluster.scale(1)
    # cluster.adapt(minimum=1, maximum=6)
    logging = prefect.context.get("logger")
    logging.debug("Dask cluster started")
    logging.debug(f"see dashboard {cluster.dashboard_link}")
    return cluster


def command_loc(cmd: str) -> str:
    """
    Given the name of a program that is assumed to be in the current path,
    return the full path by using the `shutil.which()` operation. It is
    *assumed* that `shutil` is available and the command is on the path
    :param cmd: str, the command to be run, often part of the `IMOD` package
    :return: str, the full path to the program
    """
    cmd_path = shutil.which(cmd)
    if not cmd_path:
        # if you can't find the command, pass back whatever was passed in.
        # let the runtime throw an error, and this will end up in the logs.
        cmd_path = cmd
    return cmd_path


class Config:
    # location in RML HPC
    bioformats2raw = os.environ.get(
        "BIORFORMATS2RAW_LOC",
        "/gs1/apps/user/spack-0.16.0/spack/opt/spack/linux-centos7-sandybridge/gcc-8.3.1/bioformats2raw-0.7.0-7kt7dff7f7fxmdjdk57u6xjuzmsxqodn/bin/bioformats2raw",
    )
    brt_binary = os.environ.get("BRT_LOC", "/opt/rml/imod/bin/batchruntomo")
    header_loc = os.environ.get("HEADER_LOC", "/opt/rml/imod/bin/header")
    mrc2tif_loc = os.environ.get("MRC2TIF_LOC", "/opt/rml/imod/bin/mrc2tif")
    newstack_loc = os.environ.get("NEWSTACK_LOC", "/opt/rml/imod/bin/newstack")

    # All settings moved to respective constants file
    # fibsem_input_exts = ["TIFF", "tiff", "TIF", "tif"]

    SLURM_EXECUTOR = DaskExecutor(cluster_class=SLURM_exec)
    user = os.environ["USER"]
    tmp_dir = f"/gs1/Scratch/{user}_scratch/"

    @staticmethod
    def _mount_point(share_name: str) -> str:
        share = NFS_MOUNT.get(share_name)
        if not share:
            raise signals.FAIL(f"{share_name} is not a valid name. Failing!")
        elif not Path(share).exists():
            raise signals.FAIL(f"{share_name} doesn't exist. Failing!")
        return share

    @staticmethod
    def proj_dir(share_name: str) -> str:
        """
        :param share_name: FileShareEnum string
        :return: Projects folder mount point based on the file-share name
        """
        return f"{Config._mount_point(share_name)}/Projects/"

    @staticmethod
    def assets_dir(share_name: str) -> str:
        return f"{Config._mount_point(share_name)}/Assets/"

    repo_dir = Path(os.path.dirname(__file__))
    template_dir = Path(f"{repo_dir.as_posix()}/templates")
