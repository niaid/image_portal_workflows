from pathlib import Path
from em_workflows.file_path import FilePath
import pytools
import SimpleITK
from prefect import flow, task

from em_workflows.utils import utils
from em_workflows.config import Config


@task
def convert_png_to_tiff(file_path: FilePath):
    """
    convert input.png -background white -alpha remove -alpha off ouput.tiff
    """
    input_png = file_path.fp_in.as_posix()
    output_tiff = f"{file_path.working_dir}/{file_path.base}.tiff"
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_tiff.log"
    cmd = [
        "convert",
        input_png,
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
    cmd = [
        Config.bioformats2raw,
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
    cmd = ["zarr_rechunk", "--chunk-size", "512", output_zarr]
    log_fp = f"{file_path.working_dir}/{file_path.base}_rechunk.log"
    FilePath.run(cmd, log_fp)
    asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))
    ng_asset = file_path.gen_asset(asset_type="neuroglancerZarr", asset_fp=asset_fp)
    return ng_asset


@task
def gen_thumb(file_path: FilePath):
    input_zarr = f"{file_path.working_dir}/{file_path.base}.zarr"
    output_jpeg_sm = f"{file_path.working_dir}/{file_path.base}_sm.jpeg"
    output_jpeg_lg = f"{file_path.working_dir}/{file_path.base}_lg.jpeg"

    sitk_image_sm = pytools.zarr_extract_2d(
        input_zarr,
        target_size_x=Config.SMALL_THUMB_X,
        target_size_y=Config.SMALL_THUMB_Y,
    )
    sitk_image_lg = pytools.zarr_extract_2d(
        input_zarr,
        target_size_x=Config.LARGE_THUMB_X,
        target_size_y=Config.LARGE_THUMB_Y,
    )
    utils.log(f"trying to create {output_jpeg_sm}")
    SimpleITK.WriteImage(
        sitk_image_sm,
        output_jpeg_sm,
        useCompression=True,
        compressionLevel=Config.JPEG_QUAL,
    )
    utils.log(f"trying to create {output_jpeg_lg}")
    SimpleITK.WriteImage(
        sitk_image_lg,
        output_jpeg_lg,
        useCompression=True,
        compressionLevel=Config.JPEG_QUAL,
    )
    asset_fp_sm = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg_sm))
    asset_fp_lg = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg_lg))
    thumb_asset = file_path.gen_asset(asset_type="thumbnail", asset_fp=asset_fp_sm)
    keyImage_asset = file_path.gen_asset(asset_type="keyImage", asset_fp=asset_fp_lg)
    return [thumb_asset, keyImage_asset]


@flow
def lrg_2d_flow(
    input_dir: str,
    file_name: str = None,
    callback_url: str = None,
    token: str = None,
    no_api: bool = None,
    # keep workdir if set true, useful to look at outputs
    keep_workdir: bool = False,
):
    """
    -list all png inputs (assumes all are "large")
    -create tmp dir for each.
    -convert to tiff -> zarr -> jpegs (thumb)
    """
    input_dir_fp = utils.get_input_dir(input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        Config.valid_lrg_2d_rgb_inputs,
        single_file=file_name,
    )
    fps = utils.gen_fps(input_dir=input_dir_fp, fps_in=input_fps)
    tiffs = convert_png_to_tiff.map(file_path=fps)
    zarr_assets = bioformats_gen_zarr.map(file_path=fps, wait_for=[tiffs])
    thumb_assets = gen_thumb.map(file_path=fps, wait_for=[zarr_assets])
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    callback_with_thumbs = utils.add_asset.map(prim_fp=prim_fps, asset=thumb_assets)
    callback_with_pyramids = utils.add_asset.map(
        prim_fp=callback_with_thumbs, asset=zarr_assets
    )
    filtered_callback = utils.filter_results(callback_with_pyramids)

    callback_state = utils.send_callback_body(
        token=token,
        callback_url=callback_url,
        no_api=no_api,
        files_elts=filtered_callback,
    )
    utils.cleanup_workdir(fps, wait_for=[callback_state])

    utils.log(f"callback_state = {callback_state}")
    utils.notify_api_completion(
        callback_state, token, callback_url, no_api, wait_for=callback_state
    )
    utils.log("COMPLETED")
