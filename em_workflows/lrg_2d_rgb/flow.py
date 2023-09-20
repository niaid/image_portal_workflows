import os
from pathlib import Path
import SimpleITK as sitk
from pytools import HedwigZarrImage, HedwigZarrImages
from prefect import Flow, task, Parameter
from prefect.run_configs import LocalRun

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.constants import AssetType, BIOFORMATS_NUM_WORKERS, JAVA_MAX_HEAP_SIZE
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
def bioformats_gen_zarr(file_path: FilePath):
    """
    TODO, refactor this into ng.gen_zarr
    bioformats2raw --scale-format-string '%2$d' --downsample-type AREA
    --compression=blosc --compression-properties cname=zstd
    --compression-properties clevel=5 --compression-properties shuffle=1
    input.tiff output.zarr
    """

    input_tiff = f"{file_path.working_dir}/{file_path.base}.tiff"
    output_zarr = f"{file_path.working_dir}/{file_path.base}.zarr"
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_zarr.log"
    # increase java max heap size for bf2raw command (just in case)
    os.environ["_JAVA_OPTIONS"] = JAVA_MAX_HEAP_SIZE
    cmd = [
        LRG2DConfig.bioformats2raw,
        f"--max_workers={BIOFORMATS_NUM_WORKERS}",
        "--overwrite",
        "--scale-format-string",
        "%2$d",
        "--downsample-type",
        "AREA",
        "--compression=blosc",
        "--compression-properties",
        "cname=zstd",
        "--compression-properties",
        "clevel=5",
        "--compression-properties",
        "shuffle=1",
        input_tiff,
        output_zarr,
    ]
    FilePath.run(cmd, log_fp)
    zarr_images = HedwigZarrImages(zarr_path=Path(output_zarr), read_only=False)
    zarr_image: HedwigZarrImage = zarr_images[list(zarr_images.get_series_keys())[0]]
    zarr_image.rechunk(512)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))
    ng_asset = file_path.gen_asset(
        asset_type=AssetType.NEUROGLANCER_ZARR, asset_fp=asset_fp
    )
    ng_asset["metadata"] = dict(
        shader=zarr_image.shader_type,
        dimensions=zarr_image.dims,
        # shaderParameters=zarr_image.neuroglancer_shader_parameters()
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


with Flow(
    "lrg_2d_color",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=LRG2DConfig.SLURM_EXECUTOR,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:
    """
    -list all png inputs (assumes all are "large")
    -create tmp dir for each.
    -convert to tiff -> zarr -> jpegs (thumb)
    """
    file_share = Parameter("file_share")
    input_dir = Parameter("input_dir")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url", default=None)()
    token = Parameter("token", default=None)()
    no_api = Parameter("no_api", default=None)()
    keep_workdir = Parameter("keep_workdir", default=False)()
    input_dir_fp = utils.get_input_dir(share_name=file_share, input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        VALID_LRG_2D_RGB_INPUTS,
        single_file=file_name,
    )
    fps = utils.gen_fps(share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps)
    tiffs = convert_png_to_tiff.map(file_path=fps)
    zarr_assets = bioformats_gen_zarr.map(file_path=fps, upstream_tasks=[tiffs])
    thumb_assets = gen_thumb.map(file_path=fps, upstream_tasks=[zarr_assets])
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_pyramids = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=zarr_assets
    )
    filtered_callback = utils.filter_results(callback_with_pyramids)

    cb = utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=filtered_callback
    )
    rm_workdirs = utils.cleanup_workdir(fps, upstream_tasks=[cb])
