import os
from pathlib import Path
from dask_jobqueue import SLURMCluster
from prefect.executors import DaskExecutor
import prefect


def SLURM_exec():
    cluster = SLURMCluster(n_workers=4)
    logging = prefect.context.get("logger")
    logging.debug(f"Dask cluster started")
    logging.debug(f"see dashboard {cluster.dashboard_link}")
    return cluster


class Config:
    # location in container
    dm2mrc_loc = "/opt/rml/imod/bin/dm2mrc"
    mrc2tif_loc = "/opt/rml/imod/bin/mrc2tif"
    tif2mrc_loc = "/opt/rml/imod/bin/tif2mrc"
    xfalign_loc = "/opt/rml/imod/bin/xfalign"
    xftoxg_loc = "/opt/rml/imod/bin/xftoxg"
    newstack_loc = "/opt/rml/imod/bin/newstack"
    convert_loc = "/usr/bin/convert"
    # environment where the app gets run - used for share selection
    env_to_share = {
        "dev": "RMLEMHedwigDev",
        "qa": "RMLEMHedwigQA",
        "prod": "RMLEMHedwigProd",
    }

    size_lg = "1024x1024"
    size_sm = "300x300"
    # the path to the Projects dir - can vary depending on mount point.
    # assets_dir = "/hedwigqa_data/Assets/"
    valid_2d_input_exts = [
        "DM4",
        "DM3",
        "dm4",
        "dm3",
        "TIF",
        "TIFF",
        "JPEG",
        "PNG",
        "JPG",
        "tif",
        "tiff",
        "jpeg",
        "png",
        "jpg",
    ]
    fibsem_input_exts = ["TIFF", "tiff", "TIF", "tif"]

    SLURM_EXECUTOR = DaskExecutor(cluster_class=SLURM_exec)
    brt_binary = "/opt/rml/imod/bin/batchruntomo"
    tmp_dir = "/gs1/Scratch/macmenaminpe_scratch/"
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
        return f"{Config.mount_point}{share}/Assets"

    # repo_dir = os.path.join(os.path.dirname(__file__), "..")
    repo_dir = Path(os.path.dirname(__file__))
    template_dir = Path(f"{repo_dir.as_posix()}/templates")
