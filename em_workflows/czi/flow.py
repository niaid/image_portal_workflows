#!/usr/bin/env python3
"""
Immunofluorescence (IF) microscopy overview:
-----------------------------------------------------

    - The CZI file format has been developed by ZEISS to store multidimensional IF images such as,
    z-stacks, time lapse, and multiposition experiments.
    - We rely on applications like OME Bio-Formats / OMERO, Fiji, python-bioformats to view and process czi files
    - The IF images are visualized with neuroglancer
    - The file also consists of metadata along with label and macro (RGB) images

Pipeline overview:
------------------
    - Convert czi file to OME-NGFF zarr format with OME XML
    - Generate neuroglancer meta-data from zarr sub-image for each IF image
    - Create thumbnail image from zarr label sub-image
"""
import asyncio
from pathlib import Path
from typing import List, Dict, Optional

import SimpleITK as sitk
from prefect import flow, task
from pytools.HedwigZarrImage import HedwigZarrImage
from pytools.HedwigZarrImages import HedwigZarrImages

from em_workflows.file_path import FilePath
from em_workflows.utils import utils
from em_workflows.utils import neuroglancer as ng
from em_workflows.czi.constants import (
    VALID_CZI_INPUTS,
    THUMB_X_DIM,
    THUMB_Y_DUM,
    SITK_COMPRESSION_LVL,
    TILE_SIZE,
)
from em_workflows.czi.config import CZIConfig


def gen_thumb(image: HedwigZarrImage, file_path: FilePath, image_name: str) -> dict:
    """
    Uses SimpleITK to extract and write jpeg thumbnail image for the zarr subimage
    """
    sitk_image = image.extract_2d(
        target_size_x=THUMB_X_DIM, target_size_y=THUMB_Y_DUM, auto_uint8=True
    )
    if sitk_image:
        output_jpeg = f"{file_path.working_dir}/{file_path.base}_{image_name}_sm.jpeg"
        if image_name == "label image":
            # Rotate the image 90 CW
            sitk_image = sitk.Flip(sitk.PermuteAxes(sitk_image, [1, 0]), [True, False])
        sitk.WriteImage(
            sitk_image,
            output_jpeg,
            useCompression=True,
            compressionLevel=SITK_COMPRESSION_LVL,
        )
        asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg))
        thumb_asset = file_path.gen_asset(asset_type="thumbnail", asset_fp=asset_fp)
        return thumb_asset


@task
def rechunk_zarr(file_path: FilePath) -> None:
    """
    Re-chunk the ZARR structure so that multi-channel/RGB channels are not split
    between chunks with the zarr_rechunk command provided by tomojs-pytools.
    """
    ng.rechunk_zarr(file_path=file_path)


@task
def copy_zarr_to_assets_dir(file_path: FilePath) -> None:
    """
    Copy the zarr files generated from czi files using bioformats2raw to assets folder
    """
    output_zarr = Path(f"{file_path.working_dir}/{file_path.base}.zarr")
    file_path.copy_to_assets_dir(fp_to_cp=Path(output_zarr))


@task
def generate_imageset(file_path: FilePath) -> List[Dict]:
    """
    | ImageSet consists of all the assets for a particular zarr sub-image and label images
    | Macro image is ignored
    | Label image is added as an thumbnail asset
    | Zarr sub-images are added as neurglancerZarr asset along with their metadata
    | Metadata includes:
        - shader
        - dimensions
        - shaderParameters
    """
    zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
    image_set = list()
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

        if image_name == "label image":
            assets.append(
                gen_thumb(image=image, file_path=file_path, image_name=image_name)
            )

        else:
            ng_asset = file_path.gen_asset(
                asset_type="neuroglancerZarr", asset_fp=Path(zarr_fp) / zarr_idx
            )
            # note - dims should be image.dims, but GUI does not want XYC
            # hardcoding in XY for now.
            ng_asset["metadata"] = dict(
                shader=image.shader_type,
                dimensions="XY",
                shaderParameters=image.neuroglancer_shader_parameters(
                    middle_quantile=(0.01, 0.99)
                ),
            )
            assets.append(ng_asset)
        image_elt["assets"] = assets
        image_set.append(image_elt)
    return image_set


@flow(
    name="SubFlow: Generate czi imageset",
    log_prints=True,
    task_runner=CZIConfig.HIGH_SLURM_EXECUTOR,
)
async def generate_czi_imageset(file_path: FilePath) -> List[Dict]:
    """
    Subflow for heavy lifting operation

    Overview:
        - Generate zarr from czi
        - Rechunk zarr files
        - Copy zarr files to assets folder
        - Generate imageset for the assets
    """
    zarr_result = generate_zarr.submit(file_path)
    rechunk_result = rechunk_zarr.submit(file_path, wait_for=[zarr_result])
    copy_to_assets = copy_zarr_to_assets_dir.submit(
        file_path, wait_for=[rechunk_result]
    )
    return generate_imageset.submit(file_path, wait_for=[copy_to_assets])


@task
def generate_zarr(file_path: FilePath):
    """
    Uses bioformats2raw command to convert czi file to OME-NGFF zarr file
    """
    input_czi = f"{file_path.proj_dir}/{file_path.base}.czi"
    ng.bioformats_gen_zarr(
        file_path=file_path,
        input_fname=input_czi,
        width=TILE_SIZE,
        height=TILE_SIZE,
    )


@task
def find_thumb_idx(callback: List[Dict]) -> List[Dict]:
    """
    Locate the index of label image in the image set
    """
    for elt in callback:
        for i, image_elt in enumerate(elt["imageSet"]):
            if image_elt["imageName"] == "label image":
                elt["thumbnailIndex"] = i
    return callback


@task
def update_file_metadata(file_path: FilePath, callback_with_zarr: Dict) -> Dict:
    """
    OME-xml metadata can be informative for developers to understand why the
    neuroglancer view is not appropriately rendering. This function attaches
    xml file location as the file metadata to the zarr group
    """
    zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
    zarr_images = HedwigZarrImages(Path(zarr_fp))
    ome_xml_path = zarr_images.ome_xml_path
    if ome_xml_path:
        xml_path = ome_xml_path.relative_to(file_path.asset_root)
        if callback_with_zarr["fileMetadata"] is None:
            callback_with_zarr["fileMetadata"] = dict()
        callback_with_zarr["fileMetadata"]["omeXml"] = xml_path.as_posix()
    return callback_with_zarr


@flow(
    name="IF CZI",
    flow_run_name=utils.generate_flow_run_name,
    log_prints=True,
    task_runner=CZIConfig.SLURM_EXECUTOR,
    on_completion=[utils.notify_api_completion],
    on_failure=[utils.notify_api_completion],
)
async def czi_flow(
    file_share: str,
    input_dir: str,
    file_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    token: Optional[str] = None,
    no_api: bool = False,
    keep_workdir: bool = False,
):
    input_dir_fp = utils.get_input_dir(share_name=file_share, input_dir=input_dir)

    input_fps = utils.list_files(
        input_dir_fp,
        VALID_CZI_INPUTS,
        single_file=file_name,
    )
    fps = utils.gen_fps(share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps)
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    imageSets = await asyncio.gather(
        *[generate_czi_imageset(file_path=fp) for fp in fps]
    )
    callback_with_zarrs = utils.add_imageSet.map(prim_fp=prim_fps, imageSet=imageSets)
    callback_with_zarrs = update_file_metadata.map(
        file_path=fps, callback_with_zarr=callback_with_zarrs
    )
    callback_with_zarrs = find_thumb_idx(callback=callback_with_zarrs)

    utils.callback_with_cleanup(
        fps=fps,
        callback_result=callback_with_zarrs,
        no_api=no_api,
        callback_url=callback_url,
        token=token,
        keep_workdir=keep_workdir,
    )
