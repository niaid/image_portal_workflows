import json
from typing import Dict, Optional
from pathlib import Path

import SimpleITK as sitk
from pytools import HedwigZarrImage, HedwigZarrImages
from prefect import flow, task
from prefect.states import Completed, Failed

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


@task
def convert_png_to_tiff(file_path: FilePath):
    """
    convert input.png -background white -alpha remove -alpha off ouput.tiff
    Adding argument: -define tiff:tile-geometry=128x128
    """
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


@task
def gen_zarr(file_path: FilePath) -> None:
    input_tiff = f"{file_path.working_dir}/{file_path.base}.tiff"

    ng.bioformats_gen_zarr(
        file_path=file_path,
        input_fname=input_tiff,
    )


@task
def rechunk_zarr(file_path: FilePath):
    ng.rechunk_zarr(file_path=file_path)


@task
def copy_zarr_to_assets_dir(file_path: FilePath):
    output_zarr = Path(f"{file_path.working_dir}/{file_path.base}.zarr")
    file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))


@task
def generate_ng_asset(file_path: FilePath) -> Dict:
    # Note; the seemingly redundancy of working and asset fp here.
    # However asset fp is in the network file system and is deployed for access to the users
    # Working fp is actually used for getting the metadata

    asset_fp = Path(f"{file_path.assets_dir}/{file_path.base}.zarr")
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
    return ng_asset


@task
def gen_thumb(file_path: FilePath):
    input_zarr = f"{file_path.working_dir}/{file_path.base}.zarr"
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
    return [thumb_asset, keyImage_asset]


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
    fps = utils.gen_fps.submit(
        share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps
    )
    tiffs = convert_png_to_tiff.map(file_path=fps)
    zarrs = gen_zarr.map(file_path=fps, wait_for=[tiffs])
    rechunk = rechunk_zarr.map(file_path=fps, wait_for=[zarrs])
    copy_to_assets = copy_zarr_to_assets_dir.map(file_path=fps, wait_for=[rechunk])
    zarr_assets = generate_ng_asset.map(file_path=fps, wait_for=[copy_to_assets])
    thumb_assets = gen_thumb.map(file_path=fps, wait_for=[zarr_assets])
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_pyramids = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=zarr_assets
    )

    callback_result = list()
    for cb in callback_with_pyramids:
        state = cb.wait()
        try:
            if state.is_completed():
                json.dumps(cb.result())
                callback_result.append(cb.result())
        except TypeError:  # can't serialize the item
            utils.log(f"Following item cannot be added to callback:\n\n{cb.result()}")

    utils.send_callback_body.submit(
        x_no_api=x_no_api,
        token=token,
        callback_url=callback_url,
        files_elts=callback_result,
    )

    if callback_result:
        return Completed(message="At least one callback is correct!")
    return Failed(message="None of the files succeeded!")
