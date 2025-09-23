import logging
import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from dask_jobqueue import SLURMCluster
from prefect_dask.task_runners import DaskTaskRunner
import pytools

from em_workflows.constants import NFS_MOUNT

# loads .env file into os.environ
load_dotenv()


def setup_pytools_log():
    pytools.logger.setLevel(logging.DEBUG)
    BASIC_FORMAT = "%(levelname)s:%(name)s:%(message)s"
    formatter = logging.Formatter(BASIC_FORMAT)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    pytools.logger.addHandler(handler)


def SLURM_exec(asynchronous: bool = False, **cluster_kwargs):
    """
    brings up a dynamically sized cluster.

    Docs: https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html

    We can view the sbatch script using the following command, to know how the job is started
    by slurm:
    python -c "from em_workflows import config; c = config.SLURM_exec(); print(c.job_script())"

    The processes determins number of dask workers, and nthreads = cores / processes
    The memory limit is also divided among the workers

    More about the cluster: https://bigskywiki.niaid.nih.gov/big-sky-architecture
    """
    home = os.environ["HOME"]
    env_name = os.environ["HEDWIG_ENV"]
    flowrun_id = os.environ.get("PREFECT__FLOW_RUN_ID", "not-found")
    current_dir = cluster_kwargs.pop("current_dir", home)
    job_script_prologue = [
        f"source /data/home/svc_hpchedwig_{env_name}/image_portal_workflows/.venv/bin/activate",
        "export IMOD_DIR=/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/imod-5.1.1-vyv6iidgdilzyxoqumqmdbyokzi4cdlx/IMOD",
        "export JAVA_OPTS='-Djava.io.tmpdir=/data/scratch'",
        f"export PYTHONPATH={current_dir};$PYTHONPATH",
        "echo $PATH",
    ]
    cluster = SLURMCluster(
        name="dask-worker",
        # processes=4,
        death_timeout=121,
        local_directory=f"{home}/dask_tmp/",
        log_directory=f"{home}/slurm-log/{flowrun_id}",
        job_script_prologue=job_script_prologue,
        # queue is arg for SBATCH --partition
        # to learn more about partitions, run `sinfo` in hpc
        queue="all",
        walltime="4:00:00",
        # job_extra_directives=["--gres=gpu:1"],
        asynchronous=asynchronous,
        **cluster_kwargs,
    )
    cluster.scale(5)
    # cluster.adapt(minimum=1, maximum=6)
    # to get logger, we must be within an active flow/task run
    print("Dask cluster started")
    print(f"see dashboard {cluster.dashboard_link}")
    return cluster


class Config:
    # location in RML HPC
    imod_root = "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/imod-5.1.1-vyv6iidgdilzyxoqumqmdbyokzi4cdlx/IMOD/"
    bioformats2raw = os.environ.get(
        "BIOFORMATS2RAW_LOC",
        "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/bioformats2raw-0.9.4-yj7uyq6r7zduyd34h75nt6kootcuxrg4/bioformats2raw/bin/bioformats2raw",
    )
    brt_binary = os.environ.get("BRT_LOC", f"{imod_root}/bin/batchruntomo")
    header_loc = os.environ.get("HEADER_LOC", f"{imod_root}/bin/header")
    mrc2tif_loc = os.environ.get("MRC2TIF_LOC", f"{imod_root}/bin/mrc2tif")
    newstack_loc = os.environ.get("NEWSTACK_LOC", f"{imod_root}/bin/newstack")
    ffmpeg_loc = "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/ffmpeg-6.0-zq2bmekz3iolxjshigm6b6q2w64kn5h2/bin/ffmpeg"

    # Location of GraphicsMagick binary
    #
    gm_loc = os.environ.get("GM_LOC", "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/graphicsmagick-1.3.43-5cc6lqtchmgntmy66i56rs55nk6aqopp/bin/gm")

    HIGH_SLURM_EXECUTOR = DaskTaskRunner(
        cluster_class=SLURM_exec,
        cluster_kwargs=dict(
            cores=64,
            memory="512G",
        ),
    )
    SLURM_EXECUTOR = DaskTaskRunner(
        cluster_class=SLURM_exec,
        cluster_kwargs=dict(
            cores=20,
            memory="256G",
        ),
    )

    @staticmethod
    def get_slurm_task_runner(current_dir: Path):
        return DaskTaskRunner(
            cluster_class=SLURM_exec,
            cluster_kwargs=dict(
                cores=20,
                memory="256G",
                current_dir=current_dir
            ),
        )
    user = os.environ["USER"]
    tmp_dir = f"/data/scratch/{user}"

    @staticmethod
    def _mount_point(share_name: str) -> str:
        share = NFS_MOUNT.get(share_name)
        if not share:
            raise RuntimeError(f"{share_name} is not a valid name. Failing!")
        elif not Path(share).exists():
            raise RuntimeError(f"{share_name} doesn't exist. Failing!")
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
