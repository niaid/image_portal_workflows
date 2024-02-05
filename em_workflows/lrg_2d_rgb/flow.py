from typing import Optional
from pathlib import Path

import SimpleITK as sitk
from pytools import HedwigZarrImage, HedwigZarrImages
from prefect import flow, task, unmapped

from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng
from em_workflows.file_path import FilePath
from em_workflows.constants import AssetType
from em_workflows.lrg_2d_rgb.config import LRG2DConfig
from em_workflows.lrg_2d_rgb.constants import (
    LARGE_THUMB_X,
    LARGE_THUMB_Y,
    SMALL_THUMB_X,
    SMALL_THUMB_Y,
    JPEG_QUAL,
    VALID_LRG_2D_RGB_INPUTS,
)
from em_workflows.utils import task_io
from em_workflows.utils.task_io import taskio_handler, TaskIO, gen_taskio


@task(name="convert_png_to_tiff")
@taskio_handler
def convert_png_to_tiff(taskio: TaskIO) -> TaskIO:
    """
    convert input.png -background white -alpha remove -alpha off ouput.tiff
    Adding argument: -define tiff:tile-geometry=128x128
    """
    file_path = taskio.file_path
    input_png = file_path.fp_in.as_posix()
    output_tiff = f"{file_path.working_dir}/{file_path.base}.tiff"
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_tiff.log"
    cmd = [
        "convert",
        input_png,
        "-define",
        "tiff:tile-geometry=128x128",
        "-background",
        "white",
        "-alpha",
        "remove",
        "-alpha",
        "off",
        output_tiff,
    ]
    utils.log(f"Generated cmd {cmd}")
    FilePath.run(cmd, log_fp)
    return TaskIO(output_path=Path(output_tiff))


@task(
    name="Zarr generation",
    on_failure=[utils.collect_exception_task_hook],
)
@taskio_handler
def gen_zarr(taskio: TaskIO) -> TaskIO:
    file_path = taskio.file_path
    input_tiff = taskio.output_path

    output_path = ng.bioformats_gen_zarr(
        file_path=file_path,
        input_fname=input_tiff.as_posix(),
    )
    return TaskIO(output_path=Path(output_path))


@task(
    name="Zarr rechunk",
    on_failure=[utils.collect_exception_task_hook],
)
@taskio_handler
def rechunk_zarr(taskio: TaskIO) -> TaskIO:
    ng.rechunk_zarr(file_path=taskio.file_path)
    # zarr is rechunked in-place
    return TaskIO(output_path=taskio.output_path)


@task(name="copy_zarr_to_assets_dir")
@taskio_handler
def copy_zarr_to_assets_dir(taskio: TaskIO) -> TaskIO:
    output_zarr = taskio.output_path
    asset_path = taskio.file_path.copy_to_assets_dir(fp_to_cp=output_zarr)
    return TaskIO(output_path=asset_path)


@task(
    name="Neuroglancer asset generation",
    on_failure=[utils.collect_exception_task_hook],
)
@taskio_handler
def generate_ng_asset(taskio: TaskIO) -> TaskIO:
    # Note; the seemingly redundancy of working and asset fp here.
    # However asset fp is in the network file system and is deployed for access to the users
    # Working fp is actually used for getting the metadata

    file_path = taskio.file_path
    asset_fp = taskio.output_path

    working_fp = Path(f"{file_path.working_dir}/{file_path.base}.zarr")
    hw_images = HedwigZarrImages(zarr_path=working_fp, read_only=False)
    hw_image = hw_images[list(hw_images.get_series_keys())[0]]

    # NOTE: this could be replaced by hw_image.path
    # but hw_image is part of working dir (temporary)
    first_zarr_arr = asset_fp / "0"

    ng_asset = file_path.gen_asset(
        asset_type=AssetType.NEUROGLANCER_ZARR, asset_fp=first_zarr_arr
    )
    # the GUI / API does not want to see dims XYC, will set to 'XY' for now
    # should be hw_image.dims
    ng_asset["metadata"] = dict(
        shader=hw_image.shader_type,
        dimensions="XY",
        shaderParameters=hw_image.neuroglancer_shader_parameters(),
    )
    return TaskIO(
        output_path=None,
        data=ng_asset,
    )


@task(name="gen_thumb")
@taskio_handler
def gen_thumb(taskio: TaskIO) -> TaskIO:
    input_zarr = taskio.output_path
    file_path = taskio.file_path

    zarr_images = HedwigZarrImages(zarr_path=Path(input_zarr), read_only=False)
    zarr_image: HedwigZarrImage = zarr_images[list(zarr_images.get_series_keys())[0]]

    sitk_image_sm: sitk.Image = zarr_image.extract_2d(
        target_size_x=SMALL_THUMB_X, target_size_y=SMALL_THUMB_Y
    )
    sitk_image_lg: sitk.Image = zarr_image.extract_2d(
        target_size_x=LARGE_THUMB_X, target_size_y=LARGE_THUMB_Y
    )
    output_jpeg_sm = f"{file_path.working_dir}/{file_path.base}_sm.jpeg"
    utils.log(f"trying to create {output_jpeg_sm}")
    sitk.WriteImage(
        sitk_image_sm,
        output_jpeg_sm,
        useCompression=True,
        compressionLevel=JPEG_QUAL,
    )
    output_jpeg_lg = f"{file_path.working_dir}/{file_path.base}_lg.jpeg"
    utils.log(f"trying to create {output_jpeg_lg}")
    sitk.WriteImage(
        sitk_image_lg,
        output_jpeg_lg,
        useCompression=True,
        compressionLevel=JPEG_QUAL,
    )
    asset_fp_sm = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg_sm))
    asset_fp_lg = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg_lg))
    thumb_asset = file_path.gen_asset(
        asset_type=AssetType.THUMBNAIL, asset_fp=asset_fp_sm
    )
    keyImage_asset = file_path.gen_asset(
        asset_type=AssetType.KEY_IMAGE, asset_fp=asset_fp_lg
    )
    return TaskIO(
        output_path=None,
        data=[thumb_asset, keyImage_asset],
    )


@flow(
    name="Large 2d RGB",
    flow_run_name=utils.generate_flow_run_name,
    log_prints=True,
    task_runner=LRG2DConfig.SLURM_EXECUTOR,
    on_completion=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_failure=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
)
# run_config=LocalRun(labels=[utils.get_environment()]),
def lrg_2d_flow(
    file_share: str,
    input_dir: str,
    x_file_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    token: Optional[str] = None,
    x_no_api: bool = False,
    x_keep_workdir: bool = False,
):
    """
    -list all png inputs (assumes all are "large")
    -create tmp dir for each.
    -convert to tiff -> zarr -> jpegs (thumb)
    """
    utils.notify_api_running(x_no_api, token, callback_url)

    input_dir_fp = utils.get_input_dir.submit(
        share_name=file_share, input_dir=input_dir
    )

    input_fps = utils.list_files.submit(
        input_dir_fp,
        VALID_LRG_2D_RGB_INPUTS,
        single_file=x_file_name,
    )
    fps = gen_taskio.map(
        share_name=unmapped(file_share),
        input_dir=unmapped(input_dir_fp),
        fp_in=input_fps.result(),
    )
    tiffs = convert_png_to_tiff.map(taskio=fps)
    zarrs = gen_zarr.map(taskio=tiffs)
    rechunk = rechunk_zarr.map(taskio=zarrs)
    copy_to_assets = copy_zarr_to_assets_dir.map(taskio=rechunk)
    zarr_assets = generate_ng_asset.map(taskio=copy_to_assets)
    thumb_assets = gen_thumb.map(taskio=zarrs)
    callback_with_assets = task_io.gen_response.submit(
        fps, [*zarr_assets, *thumb_assets]
    )

    utils.send_callback_body.submit(
        x_no_api=x_no_api,
        token=token,
        callback_url=callback_url,
        files_elts=callback_with_assets,
    )
