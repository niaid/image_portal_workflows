import os
from pathlib import Path
from dask_jobqueue import SLURMCluster
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


class Config:
    # location in RML HPC
    # FIXME The locations are hard-coded
    # Check nih_3d config at https://probable-chainsaw-db6dd000.pages.github.io/API_documentation/
    binvol = "/opt/rml/imod/bin/binvol"
    bioformats2raw = "/gs1/apps/user/spack-0.16.0/spack/opt/spack/linux-centos7-sandybridge/gcc-8.3.1/bioformats2raw-0.7.0-7kt7dff7f7fxmdjdk57u6xjuzmsxqodn/bin/bioformats2raw"
    brt_binary = "/opt/rml/imod/bin/batchruntomo"
    dm2mrc_loc = "/opt/rml/imod/bin/dm2mrc"
    clip_loc = "/opt/rml/imod/bin/clip"
    convert_loc = "/usr/bin/convert"  # requires imagemagick
    header_loc = "/opt/rml/imod/bin/header"
    mrc2tif_loc = "/opt/rml/imod/bin/mrc2tif"
    newstack_loc = "/opt/rml/imod/bin/newstack"
    tif2mrc_loc = "/opt/rml/imod/bin/tif2mrc"
    xfalign_loc = "/opt/rml/imod/bin/xfalign"
    xftoxg_loc = "/opt/rml/imod/bin/xftoxg"

    # environment where the app gets run - used for share selection
    env_to_share = {
        "dev": "RMLEMHedwigDev",
        "qa": "RMLEMHedwigQA",
        "prod": "RMLEMHedwigProd",
    }

    # bioformats2raw settings
    fibsem_depth = 128
    fibsem_height = 128
    fibsem_width = 128
    brt_depth = 64
    brt_width = 256
    brt_height = 256

    # Image sizes, just large and small for now
    # used in lrg_2d_color
    LARGE_THUMB_X = 1024
    LARGE_THUMB_Y = 1024
    SMALL_THUMB_X = 300
    SMALL_THUMB_Y = 300

    # JPEG image quality
    JPEG_QUAL = 90

    LARGE_DIM = 1024
    SMALL_DIM = 300
    LARGE_2D = f"{LARGE_DIM}x{LARGE_DIM}"
    SMALL_2D = f"{SMALL_DIM}x{SMALL_DIM}"

    # List of 2D extensions we may want to process
    valid_2d_input_exts = [
        "DM4",
        "DM3",
        "dm4",
        "dm3",
        "TIF",
        "TIFF",
        "tif",
        "tiff",
        "JPEG",
        "JPG",
        "jpeg",
        "jpg",
        "PNG",
        "png",
        "mrc",
        "MRC",
    ]
    fibsem_input_exts = ["TIFF", "tiff", "TIF", "tif"]

    valid_lrg_2d_rgb_inputs = ["png", "PNG"]
    SLURM_EXECUTOR = DaskExecutor(cluster_class=SLURM_exec)
    user = os.environ["USER"]
    tmp_dir = f"/gs1/Scratch/{user}_scratch/"
    mount_point = "/mnt/ai-fas12/"

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
