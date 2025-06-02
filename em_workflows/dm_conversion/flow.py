from pathlib import Path
from typing import Optional

import SimpleITK as sitk
import SimpleITK.utilities as sitkutils

from prefect import flow, task, unmapped, allow_failure
from pytools.meta import is_16bit
from pytools.convert import file_to_uint8

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.constants import AssetType
from em_workflows.dm_conversion.config import DMConfig
from em_workflows.dm_conversion.constants import (
    LARGE_DIM,
    SMALL_DIM,
    LARGE_2D,
    SMALL_2D,
    TIFS_EXT,
    DMS_EXT,
    MRCS_EXT,
    KEYIMG_EXTS,
    VALID_2D_INPUT_EXTS,
)


def _calculate_shrink_factor(filepath: Path, enforce_2d: bool = True, target_size: int = LARGE_DIM) -> float:
    """
    Calculate the shrink factor for the newstack command to produce an image of the target size on the largest dimension.
    """
    dims = utils.lookup_dims(filepath)

    if enforce_2d and dims.z != 1:
        msg = f"mrc file {filepath} is not 2 dimensional. Contains {dims.z} Z dims."
        raise RuntimeError(msg)

    # use the max dimension of x & y to compute shrink_factor
    max_xy = max(dims.x, dims.y)
    # work out shrink_factor to make the resulting image LARGE_DIM at max
    return max_xy / target_size


def _newstack_mrc_to_tiff(input_fn: Path, output_fn: Path, log_fn: Path, use_float=True, verbose=False) -> None:
    """
    Convert mrc files to tiff using newstack while reducing the size of the image, and performing antialiasing suitable
    for EM images.

    https://bio3d.colorado.edu/imod/doc/man/newstack.html

    :param input_fn: Input file path of a mrc file.
    :param output_fn: Output file path of a tiff file.
    :param log_fn: File path to a log file for the newstack command output.
    :param use_float: If true uses the newstack "float" option to  fill the data range of the output. This range is
      computed after the shrink operation. If false, uses the "meansd" option with the default values of 140,50, which
      computed before the shrink operation.
    :param verbose: If true, the newstack command is run in normal mode. Otherwise, quiet mode is enabled.
    :return:
    """

    shrink_factor = _calculate_shrink_factor(input_fn, target_size=LARGE_DIM)

    cmd = [
        DMConfig.newstack_loc,
        "-shrink",
        f"{shrink_factor:.3f}",
        "-antialias",
        "6",
        "-mode",
        "0",
        ]

    if not verbose:
        cmd.append("-quiet")

    if use_float:
        cmd.extend(["-float", "1"])
    else:
        cmd.extend(["-meansd", "140,50"])

    cmd.extend([
        input_fn.as_posix(),
        output_fn.as_posix(),
        ])

    FilePath.run(cmd, str(log_fn), env={"IMOD_OUTPUT_FORMAT": "TIF"} )


def _write_image_as_size(img:sitk.Image, size:(int,int), output_path:Path, compression_level:int) -> None:
    """
    Resize the image to the specified size and write it to the output file path with the specified compression level.
    If the image is smaller than the specified size, it will not be resized.

    :param img: A SimpleITK image object.
    :param size:
    :param output_path: Path the output filename, the file extension should be .jpeg
    :param compression_level: The compression level for the output image. 0-100, 0 is no compression, 100 is maximum compression.
    :return:
    """
    if any(img_sz > small_sz for img_sz, small_sz in zip(img.GetSize(), size)):
        utils.log(msg=f"Resizing {img.GetSize()}->{size} for {output_path}...")
        img = sitkutils.resize(img, size, interpolator=sitk.sitkLinear, fill=False, use_nearest_extrapolator=True)
    else:
        utils.log(msg=f"Writing {output_path}...")

    sitk.WriteImage(img, output_path, useCompression=True, compressionLevel=compression_level)


@task(
    name="Convert EM images to tiff",
    on_failure=[utils.collect_exception_task_hook],
)
def convert_em_to_tiff(file_path: FilePath) -> Path:
    """
    Performs the conversion of EM images to tiff format, adjusting the dynamic range and reducing the size of the image.

    Three types of files are detected to need conversion:
      - mrc files are converted to tiff using newstack
      - 16-bit tiff files are converted to 8-bit tiff
      - dm3/dm4 files are converted to mrc using dm2mrc, then to tiff using newstack

    EM images are broadly characterized by a low signal-to-noise ratio and a high dynamic range, often with potential
     outliers. Using newstack with shrink and antialiasing kernels is required to reduce the noise to produce a
     high-quality tiff image. Additionally, to adjust the dynamic range, the meansd, or float options are used.


    :param file_path:
    :return:
    """

    out_fp = file_path.gen_output_fp(output_ext="_as_tiff.tiff")
    log_fp = file_path.gen_output_fp(output_ext="_as_tiff.log")

    if file_path.fp_in.suffix.strip(".").lower() in MRCS_EXT:

        utils.log(f"{file_path.fp_in.as_posix()} is a mrc file, will convert to {out_fp}.")

        _newstack_mrc_to_tiff(file_path.fp_in, out_fp, log_fp, use_float=False)

    elif (
            file_path.fp_in.suffix.strip(".").lower() in TIFS_EXT
            and is_16bit(file_path.fp_in)
    ):

        utils.log(f"{file_path.fp_in} is a 16 bit tiff, converting to {out_fp}")

        _newstack_mrc_to_tiff(file_path.fp_in, out_fp, log_fp, use_float=True)

    elif file_path.fp_in.suffix.strip(".").lower() in DMS_EXT:
        dm_as_mrc = file_path.gen_output_fp(output_ext="dm_as_mrc.mrc")
        utils.log(msg=f"{file_path.fp_in} is a dm file, will convert to {dm_as_mrc}.")

        mrc_log_fp = file_path.gen_output_fp(output_ext="_dm2mrc.log")

        cmd = [DMConfig.dm2mrc_loc, file_path.fp_in.as_posix(), dm_as_mrc.as_posix()]
        FilePath.run(cmd=cmd, log_file=str(mrc_log_fp))

        utils.log(f"{dm_as_mrc} convert to {out_fp}.")

        _newstack_mrc_to_tiff(dm_as_mrc, out_fp, log_fp, use_float=True)
    else:
        return file_path.fp_in

    return out_fp


@task(
    name="Generate key and thumbnail jpeg images ",
    on_failure=[utils.collect_exception_task_hook],
)
def generate_jpegs(file_path: FilePath) -> dict:
    """
    Generates small and large jpegs from the input file and produce an asset dictionary.

    The input image is converted to tiff for certain EM image, if needed.

    SimpleITK is used to the original input or the converted tiff image to generate the jpegs.

    """

    current_image_path = convert_em_to_tiff.fn(file_path)

    if not current_image_path.exists():
        raise RuntimeError(f"File {current_image_path} does not exist. Cannot convert to jpeg.")

    utils.log(msg=f"Reading {current_image_path}...")
    img = sitk.ReadImage(current_image_path)

    # Produce a small thumbnail
    output_small = file_path.gen_output_fp(output_ext="_SM.jpeg")
    _write_image_as_size(img, (SMALL_DIM, )*2, output_small, 70)

    asset_type = AssetType.THUMBNAIL
    asset_small_fp = file_path.copy_to_assets_dir(fp_to_cp=output_small)
    asset_small_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_small_fp)

    # Produce a large key image
    output_large = file_path.gen_output_fp(output_ext="_LG.jpeg")
    _write_image_as_size(img, (LARGE_DIM, )*2, output_large, 80)

    asset_type = AssetType.KEY_IMAGE
    asset_large_fp = file_path.copy_to_assets_dir(fp_to_cp=output_large)
    asset_large_elt = file_path.gen_asset(asset_type=asset_type, asset_fp=asset_large_fp)

    return utils.gen_prim_fps.fn(fp_in=file_path, additional_assets=(asset_small_elt, asset_large_elt))


@flow(
    name="Small 2D",
    flow_run_name=utils.generate_flow_run_name,
    log_prints=True,
    task_runner=DMConfig.SLURM_EXECUTOR,
    on_completion=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_failure=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
    on_crashed=[
        utils.notify_api_completion,
        utils.copy_workdirs_and_cleanup_hook,
    ],
)
def dm_flow(
    file_share: str,
    input_dir: str,
    x_file_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    token: Optional[str] = None,
    x_no_api: bool = False,
    x_keep_workdir: bool = False,
):
    """
    - List all inputs (files of a relevant input type)
    - Create output directories
    - Generate thumbnails and key images
    - Post callback to the API with the access
    """
    utils.notify_api_running(x_no_api, token, callback_url)

    # utils.log(input_dir)
    input_dir_fp = utils.get_input_dir.submit(
        share_name=file_share, input_dir=input_dir
    )

    input_fps = utils.list_files.submit(
        input_dir_fp,
        VALID_2D_INPUT_EXTS,
        single_file=x_file_name,
    )

    fps = utils.gen_fps.submit(
        share_name=file_share, input_dir=input_dir_fp, fps_in=input_fps
    )

    prim_fps = generate_jpegs.map(fps)

    callback_result = list()
    for idx, (fp, cb) in enumerate(zip(fps.result(), prim_fps)):
        state = cb.wait()
        if state.is_completed():
            callback_result.append(cb.result())
        else:
            path = f"{state.state_details.flow_run_id}__{idx}"
            try:
                message = DMConfig.local_storage.read_path(path)
                callback_result.append(fp.gen_prim_fp_elt(message.decode()))
            except ValueError:
                callback_result.append(fp.gen_prim_fp_elt("Something went wrong!"))

    utils.send_callback_body.submit(
        x_no_api=x_no_api,
        token=token,
        callback_url=callback_url,
        files_elts=callback_result,
    )
