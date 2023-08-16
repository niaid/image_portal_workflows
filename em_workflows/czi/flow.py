from pathlib import Path
from typing import List, Dict
import SimpleITK as sitk
from prefect import Flow, Parameter, task
from pytools.HedwigZarrImage import HedwigZarrImage
from pytools.HedwigZarrImages import HedwigZarrImages
from em_workflows.file_path import FilePath
from em_workflows.utils import utils
from prefect.run_configs import LocalRun
from .constants import VALID_CZI_INPUTS
from .config import CZIConfig


def rechunk_zarr(zarr_fp: Path) -> None:
    images = HedwigZarrImages(zarr_fp, read_only=False)
    for _, image in images.series():
        image.rechunk(512)  # config.CHUNK_SIZE


def gen_thumb(image: HedwigZarrImage, file_path: FilePath) -> dict:
    sitk_image = image.extract_2d(target_size_x=300, target_size_y=300, auto_uint8=True)
    if sitk_image:
        output_jpeg = f"{file_path.working_dir}/{file_path.base}_sm.jpeg"
        sitk.WriteImage(
            sitk_image,
            output_jpeg,
            useCompression=True,
            compressionLevel=90,
        )
        asset_fp = file_path.copy_to_assets_dir(fp_to_cp=Path(output_jpeg))
        thumb_asset = file_path.gen_asset(asset_type="thumbnail", asset_fp=asset_fp)
        return thumb_asset


def gen_imageSet(file_path: FilePath) -> List:

    zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
    imageSet = list()
    zarrImages = HedwigZarrImages(Path(zarr_fp))
    for image_name, image in zarrImages.series():
        # single image element
        image_elt = dict()
        image_elt["imageMetadata"] = None
        assets = list()
        if image_name == "macro image":
            # we don't care about the macro image
            continue
        assets.append(gen_thumb(image=image, file_path=file_path))
        image_elt["imageName"] = image_name

        if image_name != "label image":
            ng_asset = file_path.gen_asset(
                asset_type="neuroglancerZarr", asset_fp=Path(zarr_fp)
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
def bioformats_gen_zarr(file_path: FilePath):
    """
    TODO sort ot the num workers
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
        "--max_workers=19",
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


with Flow(
    "czi_to_zarr",
    state_handlers=[utils.notify_api_completion, utils.notify_api_running],
    executor=CZIConfig.SLURM_EXECUTOR,
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
    prim_fps = utils.gen_prim_fps.map(fp_in=fps)
    imageSets = bioformats_gen_zarr.map(file_path=fps)
    callback_with_zarrs = utils.add_imageSet.map(prim_fp=prim_fps, imageSet=imageSets)
    callback_with_zarrs = find_thumb_idx(callback=callback_with_zarrs)
    filtered_callback = utils.filter_results(callback_with_zarrs)
    cb = utils.send_callback_body(
        token=token, callback_url=callback_url, files_elts=filtered_callback
    )
    rm_workdirs = utils.cleanup_workdir(fps, upstream_tasks=[cb])