import logging
import os
from pathlib import Path
import sys
import threading

from dotenv import load_dotenv
from prefect.task_runners import ConcurrentTaskRunner
import pytools

from em_workflows.constants import NFS_MOUNT

# loads .env file into os.environ
load_dotenv()
os.environ.setdefault("JAVA_OPTS", "-Djava.io.tmpdir=/data/scratch")

# Fix SSL_CERT_FILE for Python 3.13 / httpx / anyio.
# In this HPC environment, the venv activation or module system sets SSL_CERT_FILE=""
# (empty string). ssl.create_default_context() accepts the empty string without error
# in Python 3.13, but the resulting SSL context fails at TLS read time with
# "passed invalid argument". REQUESTS_CA_BUNDLE is already set correctly in the
# service file, so mirror it into SSL_CERT_FILE so that httpx (used by Prefect
# internally) gets a valid CA bundle.
_ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE", "").strip()
if _ca_bundle:
    os.environ["SSL_CERT_FILE"] = _ca_bundle
elif not os.environ.get("SSL_CERT_FILE", "").strip():
    # Neither is set — unset SSL_CERT_FILE so Python uses the system default.
    os.environ.pop("SSL_CERT_FILE", None)


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
    python -c "from em_workflows import config; c = config.SLURM_exec(cores=8, memory='24GB'); print(c.job_script())"

    The processes determins number of dask workers, and nthreads = cores / processes
    The memory limit is also divided among the workers

    More about the cluster: https://bigskywiki.niaid.nih.gov/big-sky-architecture
    """
    home = os.environ["HOME"]
    flowrun_id = os.environ.get("PREFECT__FLOW_RUN_ID", "not-found")
    current_dir = cluster_kwargs.pop("current_dir", Config.repo_dir.parent)
    job_script_prologue = cluster_kwargs.pop(
        "job_script_prologue",
        Config.get_base_job_script_prologue(current_dir),
    )
    from dask_jobqueue import SLURMCluster

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
    bioformats2raw = os.environ.get("BIOFORMATS2RAW_LOC", "bioformats2raw")
    brt_binary = os.environ.get("BRT_LOC", "batchruntomo")
    header_loc = os.environ.get("HEADER_LOC", "header")
    mrc2tif_loc = os.environ.get("MRC2TIF_LOC", "mrc2tif")
    newstack_loc = os.environ.get("NEWSTACK_LOC", "newstack")
    ffmpeg_loc = os.environ.get("FFMPEG_LOC", "ffmpeg")
    gm_loc = os.environ.get("GM_LOC", "gm")
    java_opts = os.environ.get("JAVA_OPTS", "-Djava.io.tmpdir=/data/scratch")
    java_tool_options = os.environ.get(
        "JAVA_TOOL_OPTIONS", "-Djava.io.tmpdir=/data/scratch"
    )

    @classmethod
    def get_base_job_script_prologue(cls, current_dir: Path = None) -> list[str]:
        env_name = os.environ["HEDWIG_ENV"]
        current_dir = Path(current_dir) if current_dir is not None else cls.repo_dir.parent
        # Slurm workers need SSL_CERT_FILE set to a valid CA bundle so that
        # Prefect's httpx client (running inside the worker) can connect to the
        # Prefect API server. SSL_CERT_FILE="" (empty string) causes
        # ssl.SSLError: passed invalid argument at TLS read time in Python 3.13.
        # Use REQUESTS_CA_BUNDLE as the authoritative source since it is already
        # correctly set in the service file.
        requests_ca = os.environ.get("REQUESTS_CA_BUNDLE", "").strip()
        ssl_lines = []
        if requests_ca:
            # Set both so requests AND httpx/anyio both find the right CA bundle.
            ssl_lines.append(f"export REQUESTS_CA_BUNDLE={requests_ca}")
            ssl_lines.append(f"export SSL_CERT_FILE={requests_ca}")
        else:
            ssl_lines.append("unset REQUESTS_CA_BUNDLE")
            ssl_lines.append("unset SSL_CERT_FILE")
        return [
            f"source /gs1/home/hedwig_{env_name}/{env_name}/bin/activate"
        ]

    @classmethod
    def get_flow_job_script_prologue(cls, current_dir: Path = None) -> list[str]:
        return []

    @classmethod
    def get_job_script_prologue(cls, current_dir: Path = None) -> list[str]:
        return [
            *cls.get_base_job_script_prologue(current_dir),
            *cls.get_flow_job_script_prologue(current_dir),
        ]

    @classmethod
    def _build_task_runner(cls, cores: int, memory: str, current_dir: Path = None):
        # Dask/distributed startup can try to register signal handlers.
        # Non-main thread imports (e.g., worker deserialization) must avoid this.
        if threading.current_thread() is not threading.main_thread():
            return ConcurrentTaskRunner()

        from prefect_dask.task_runners import DaskTaskRunner

        cluster_kwargs = dict(
            cores=cores,
            memory=memory,
            job_script_prologue=cls.get_job_script_prologue(current_dir),
        )
        if current_dir is not None:
            cluster_kwargs["current_dir"] = current_dir

        return DaskTaskRunner(
            cluster_class=SLURM_exec,
            cluster_kwargs=cluster_kwargs,
        )

    @classmethod
    def get_high_slurm_task_runner(cls, current_dir: Path = None):
        return cls._build_task_runner(
            cores=60,
            memory="100G",
            current_dir=current_dir,
        )

    @classmethod
    def get_slurm_task_runner(cls, current_dir: Path = None):
        return cls._build_task_runner(
            cores=20,
            memory="256G",
            current_dir=current_dir,
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
