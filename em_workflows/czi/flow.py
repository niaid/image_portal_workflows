from pathlib import Path
from typing import List, Dict
import SimpleITK as sitk
from prefect import Flow, Parameter, task
from pytools.HedwigZarrImage import HedwigZarrImage
from pytools.HedwigZarrImages import HedwigZarrImages
from em_workflows.file_path import FilePath
from em_workflows.utils import utils
from prefect.run_configs import LocalRun
from em_workflows.czi.constants import (
    BIOFORMATS_NUM_WORKERS,
    RECHUNK_SIZE,
    VALID_CZI_INPUTS,
    THUMB_X_DIM,
    THUMB_Y_DUM,
    SITK_COMPRESSION_LVL,
)
from em_workflows.czi.config import CZIConfig


def rechunk_zarr(zarr_fp: Path) -> None:
    images = HedwigZarrImages(zarr_fp, read_only=False)
    for _, image in images.series():
        image.rechunk(RECHUNK_SIZE)


def gen_thumb(image: HedwigZarrImage, file_path: FilePath, image_name: str) -> dict:
    sitk_image = image.extract_2d(
        target_size_x=THUMB_X_DIM, target_size_y=THUMB_Y_DUM, auto_uint8=True
    )
    if sitk_image:
        output_jpeg = f"{file_path.working_dir}/{file_path.base}_{image_name}_sm.jpeg"
        sitk.WriteImage(
            sitk_image,
            output_jpeg,
            useCompression=True,
            compressionLevel=SITK_COMPRESSION_LVL,
        )
        asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg))
        thumb_asset = file_path.gen_asset(asset_type="thumbnail", asset_fp=asset_fp)
        return thumb_asset


def gen_imageSet(file_path: FilePath) -> List:
    zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
    imageSet = list()
    zarr_images = HedwigZarrImages(Path(zarr_fp))
    # for image_name, image in zarr_images.series():
    for k_idx, image_name in enumerate(zarr_images.get_series_keys()):
        # The relative path of the zarr group from the root zarr
        # this assumes a valid zarr group with OME directory inside
        ome_index_to_zarr_group = zarr_images.zarr_root["OME"].attrs["series"]
        zarr_idx = ome_index_to_zarr_group[k_idx]
        image = HedwigZarrImage(
            zarr_images.zarr_root[zarr_idx], zarr_images.ome_info, k_idx
        )
        # single image element
        image_elt = dict()
        image_elt["imageMetadata"] = None
        assets = list()
        if image_name == "macro image":
            # we don't care about the macro image
            continue
        if not image_name:
            image_name = f"Scene {k_idx}"
        image_elt["imageName"] = image_name

        assets.append(
            gen_thumb(image=image, file_path=file_path, image_name=image_name)
        )

        if image_name != "label image":
            ng_asset = file_path.gen_asset(
                asset_type="neuroglancerZarr", asset_fp=Path(zarr_fp) / zarr_idx
            )
            ng_asset["metadata"] = dict(
                shader=image.shader_type,
                dimensions=image.dims,
                shaderParameters=image.neuroglancer_shader_parameters(),
            )
            assets.append(ng_asset)
        image_elt["assets"] = assets
        imageSet.append(image_elt)
    return imageSet


@task
def generate_czi_imageset(file_path: FilePath):
    """
    TODO, refactor this into ng.gen_zarr
    bioformats2raw --max_workers=$nproc  --downsample-type AREA
    --compression=blosc --compression-properties cname=zstd
    --compression-properties clevel=5 --compression-properties shuffle=1
    input.tiff output.zarr
    """

    input_czi = f"{file_path.proj_dir}/{file_path.base}.czi"
    output_zarr = f"{file_path.working_dir}/{file_path.base}.zarr"
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_zarr.log"
    cmd = [
        CZIConfig.bioformats2raw,
        f"--max_workers={BIOFORMATS_NUM_WORKERS}",
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
    rechunk_zarr(zarr_fp=Path(output_zarr))
    file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))
    imageSet = gen_imageSet(file_path=file_path)
    # extract images from input file, used to create imageSet elements
    return imageSet


@task
def find_thumb_idx(callback: List[Dict]) -> List[Dict]:
    for elt in callback:
        for i, image_elt in enumerate(elt["imageSet"]):
            if image_elt["imageName"] == "label image":
                elt["thumbnailIndex"] = i
                return callback
    return callback


with Flow(
    "czi_to_zarr",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=CZIConfig.SLURM_EXECUTOR,
    run_config=LocalRun(labels=[utils.get_environment()]),
) as flow:
    input_dir = Parameter("input_dir")
    file_share = Parameter("file_share")
    file_name = Parameter("file_name", default=None)
    callback_url = Parameter("callback_url", default=None)()
    token = Parameter("token", default=None)()
    no_api = Parameter("no_api", default=None)()
    # keep workdir if set true, useful to look at outputs
    keep_workdir = Parameter("keep_workdir", default=False)()
    input_dir_fp = utils.get_input_dir(share_name=file_share, input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        VALID_CZI_INPUTS,
        single_file=file_name,
    )
    fps = utils.gen_fps(share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps)
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    imageSets = generate_czi_imageset.map(file_path=fps)
    callback_with_zarrs = utils.add_imageSet.map(prim_fp=prim_fps, imageSet=imageSets)
    callback_with_zarrs = find_thumb_idx(callback=callback_with_zarrs)
    filtered_callback = utils.filter_results(callback_with_zarrs)
    cb = utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=filtered_callback
    )
    rm_workdirs = utils.cleanup_workdir(fps, upstream_tasks=[cb])
