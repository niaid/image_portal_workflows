import os
from pathlib import Path
from dask_jobqueue import SLURMCluster
from prefect.executors import DaskExecutor
import prefect


def SLURM_exec():
    cluster = SLURMCluster(n_workers=30)
    logging = prefect.context.get("logger")
    logging.debug(f"Dask cluster started")
    logging.debug(f"see dashboard {cluster.dashboard_link}")
    return cluster


class Config:
    # location in container
    dm2mrc_loc = "/usr/local/IMOD/bin/dm2mrc"
    mrc2tif_loc = "/usr/local/IMOD/bin/mrc2tif"
    size_lg = "1024x1024"
    size_sm = "300x300"
    # the path to the Projects dir - can vary depending on mount point.
    # assets_dir = "/hedwigqa_data/Assets/"
    two_d_input_exts = ["dm4", "dm3", "tif", "tiff", "png", "jpg", "jpeg"]
    SLURM_EXECUTOR = DaskExecutor(cluster_class=SLURM_exec)
    brt_binary = "/opt/rml/imod/bin/batchruntomo"
    # brt_binary = "/usr/local/IMOD/bin/batchruntomo"
    # tmp_dir = "/gs1/home/macmenaminpe/tmp"
    tmp_dir = "/gs1/Scratch/macmenaminpe_scratch/"
    # tmp_dir = "/tmp/"
    mount_point = "/mnt/ai-fas12/"
    # mount_point = "/home/macmenaminpe/"
    proj_dir = f"{mount_point}/data/"
    # repo_dir = os.path.join(os.path.dirname(__file__), "..")
    repo_dir = Path(os.path.dirname(__file__))
    template_dir = Path(f"{repo_dir.as_posix()}/templates")
