import os
from pathlib import Path
from dask_jobqueue import SLURMCluster
from dotenv import load_dotenv
from prefect.executors import DaskExecutor
import prefect
import shutil


def SLURM_exec():
    """
    brings up a dynamically sized cluster.
    For some reason processes > 1 crash BRT. Be careful optimizing this.
    """
    home = os.environ["HOME"]
    cluster = SLURMCluster(
        name="dask-worker",
        cores=60,
        memory="32G",
        # processes=1,
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


class WorkflowConfig:
    _self = None

    def __new__(cls):
        # single instance only
        if cls._self is None:
            cls._self = super().__new__(cls)
        return cls._self

    def __init__(self):
        # loads .env file into os.environ
        load_dotenv()

        # location in RML HPC
        self.binvol = os.environ.get("BINVOL_LOC", "/opt/rml/imod/bin/binvol")
        self.bioformats2raw = os.environ.get(
            "BIORFORMATS2RAW_LOC",
            "/gs1/apps/user/spack-0.16.0/spack/opt/spack/linux-centos7-sandybridge/gcc-8.3.1/bioformats2raw-0.7.0-7kt7dff7f7fxmdjdk57u6xjuzmsxqodn/bin/bioformats2raw",
        )
        self.brt_binary = os.environ.get("BRT_LOC", "/opt/rml/imod/bin/batchruntomo")
        self.dm2mrc_loc = os.environ.get("DM2MRC_LOC", "/opt/rml/imod/bin/dm2mrc")
        self.clip_loc = os.environ.get("CLIP_LOC", "/opt/rml/imod/bin/clip")
        self.convert_loc = os.environ.get(
            "CONVERT_LOC", "/usr/bin/convert"
        )  # requires imagemagick
        self.header_loc = os.environ.get("HEADER_LOC", "/opt/rml/imod/bin/header")
        self.mrc2tif_loc = os.environ.get("MRC2TIF_LOC", "/opt/rml/imod/bin/mrc2tif")
        self.newstack_loc = os.environ.get("NEWSTACK_LOC", "/opt/rml/imod/bin/newstack")
        self.tif2mrc_loc = os.environ.get("TIF2MRC_LOC", "/opt/rml/imod/bin/tif2mrc")
        self.xfalign_loc = os.environ.get("XFALIGN_LOC", "/opt/rml/imod/bin/xfalign")
        self.xftoxg_loc = os.environ.get("XFTOXG_LOC", "/opt/rml/imod/bin/xftoxg")

        # environment where the app gets run - used for share selection
        self.env_to_share = {
            "dev": "RMLEMHedwigDev",
            "qa": "RMLEMHedwigQA",
            "prod": "RMLEMHedwigProd",
        }

        # bioformats2raw settings
        # All settings moved to respective constants file
        # fibsem_input_exts = ["TIFF", "tiff", "TIF", "tif"]

        self.SLURM_EXECUTOR = DaskExecutor(cluster_class=SLURM_exec)
        self.user = os.environ["USER"]
        self.tmp_dir = f"/gs1/Scratch/{self.user}_scratch/"
        self.mount_point = "/mnt/ai-fas12/"

    @staticmethod
    def _share_name(env: str) -> str:
        """
        gets the path of the location of input based on environment
        """
        val = Config.env_to_share.get(env)
        if not val:
            raise ValueError(
                f"Environment {env} not in valid environments: \
                    {Config.env_to_share.keys()}"
            )
        return val

    @staticmethod
    def proj_dir(env: str) -> str:
        share = Config._share_name(env=env)
        return f"{Config.mount_point}{share}/Projects/"

    @staticmethod
    def assets_dir(env: str) -> str:
        share = Config._share_name(env=env)
        return f"{Config.mount_point}{share}/Assets/"

    repo_dir = Path(os.path.dirname(__file__))
    template_dir = Path(f"{repo_dir.as_posix()}/templates")


Config = WorkflowConfig()
