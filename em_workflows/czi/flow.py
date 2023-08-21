from pathlib import Path
from prefect import Flow, Parameter, task
from em_workflows.file_path import FilePath
from em_workflows.utils import utils
from em_workflows.config import Config
from prefect.run_configs import LocalRun
from .constants import VALID_CZI_INPUTS


@task
def bioformats_gen_zarr(file_path: FilePath):
    """
    TODO, refactor this into ng.gen_zarr
    bioformats2raw --max_workers=$nproc  --downsample-type AREA
    --compression=blosc --compression-properties cname=zstd
    --compression-properties clevel=5 --compression-properties shuffle=1
    input.tiff output.zarr
    """

    input_czi = f"{file_path.working_dir}/{file_path.base}.czi"
    output_zarr = f"{file_path.working_dir}/{file_path.base}.zarr"
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_zarr.log"
    cmd = [
        Config.bioformats2raw,
        "--max_workers=8",
        "--overwrite",
        "--downsample-type",
        "AREA",
        "--compression=blosc",
        "--compression-properties",
        "cname=zstd",
        "--compression-properties",
        "clevel=5",
        "--compression-properties",
        "shuffle=1",
        input_czi,
        output_zarr,
    ]
    FilePath.run(cmd, log_fp)
    cmd = ["zarr_rechunk", "--chunk-size", "512", output_zarr]
    log_fp = f"{file_path.working_dir}/{file_path.base}_rechunk.log"
    FilePath.run(cmd, log_fp)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))
    ng_asset = file_path.gen_asset(asset_type="neuroglancerZarr", asset_fp=asset_fp)
    return ng_asset


with Flow(
    "czi_to_zarr",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=Config.SLURM_EXECUTOR,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:

    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url", default=None)()
    token = Parameter("token", default=None)()
    no_api = Parameter("no_api", default=None)()
    # keep workdir if set true, useful to look at outputs
    keep_workdir = Parameter("keep_workdir", default=False)()
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        VALID_CZI_INPUTS,
        single_file=file_name,
    )
    fps = utils.gen_fps(input_dir=input_dir_fp, fps_in=input_fps)
