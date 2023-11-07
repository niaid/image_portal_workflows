from pathlib import Path
from typing import List, Dict

import SimpleITK as sitk
from prefect import flow, task, allow_failure
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
)
from em_workflows.czi.config import CZIConfig


def gen_thumb(image: HedwigZarrImage, file_path: FilePath, image_name: str) -> dict:
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


def gen_imageSet(file_path: FilePath) -> List:
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


@task
def generate_czi_imageset(file_path: FilePath):
    input_czi = f"{file_path.proj_dir}/{file_path.base}.czi"
    ng.bioformats_gen_zarr(
        file_path=file_path,
        input_fname=input_czi,
        rechunk=True,
        width=512,
        height=512,
    )
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


@task
def update_file_metadata(file_path: FilePath, callback_with_zarr: Dict) -> Dict:
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
    name="Flow: IF czi",
    log_prints=True,
    task_runner=CZIConfig.SLURM_EXECUTOR,
    on_completion=[utils.notify_api_completion],
    on_failure=[utils.notify_api_completion],
)
def czi_flow(
    file_share: str,
    input_dir: str,
    file_name: str = None,
    callback_url: str = None,
    token: str = None,
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
    imageSets = generate_czi_imageset.map(file_path=fps)
    callback_with_zarrs = utils.add_imageSet.map(prim_fp=prim_fps, imageSet=imageSets)
    callback_with_zarrs = update_file_metadata.map(
        file_path=fps, callback_with_zarr=callback_with_zarrs
    )
    callback_with_zarrs = find_thumb_idx(callback=callback_with_zarrs)
    utils.copy_workdirs.map(fps, wait_for=[callback_with_zarrs])
    filtered_callback = utils.filter_results(callback_with_zarrs)
    cb = utils.send_callback_body(
        no_api=no_api,
        token=token,
        callback_url=callback_url,
        files_elts=filtered_callback,
    )
    utils.cleanup_workdir(fps, keep_workdir, wait_for=[allow_failure(cb)])
