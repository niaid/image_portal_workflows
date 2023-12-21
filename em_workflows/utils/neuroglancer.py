from pathlib import Path
from pytools.HedwigZarrImages import HedwigZarrImages
from em_workflows.config import Config
from em_workflows.file_path import FilePath
from em_workflows.constants import BIOFORMATS_NUM_WORKERS, RECHUNK_SIZE
from em_workflows.utils import utils
from em_workflows.config import setup_pytools_log

setup_pytools_log()


def rechunk_zarr(file_path: FilePath) -> None:
    zarr_fp = Path(f"{file_path.working_dir}/{file_path.base}.zarr")
    utils.log(f"{zarr_fp} output zarr")
    images = HedwigZarrImages(zarr_fp, read_only=False)
    for _, image in images.series():
        image.rechunk(RECHUNK_SIZE, in_memory=True)


def bioformats_gen_zarr_dup(
    fp_in: Path,
    rechunk: bool = False,
    width: int = None,
    height: int = None,
    resolutions: int = None,
    depth: int = None,
):
    """
    Following params alter based on what kind of flow is running...

    :param input_fname: Depending on the flow, input fname might be different
    :param rechunk: Rechunk zarrs if required
    :param width:
    :param height:
    :param resolutions:
    :param depth: These arguments are only used by BRT and SEM flows
    """
    output_zarr = f"{fp_in.parent}/{fp_in.stem}.zarr"
    log_fp = f"{fp_in.parent}/{fp_in.stem}_as_zarr.log"
    cmd = [
        Config.bioformats2raw,
        f"--max_workers={BIOFORMATS_NUM_WORKERS}",
        "--overwrite",
        "--compression",
        "blosc",
        "--compression-properties",
        "cname=zstd",
        "--compression-properties",
        "clevel=5",
        "--compression-properties",
        "shuffle=1",
    ]
    if resolutions is not None:
        cmd.extend(["--resolutions", str(resolutions)])
    if width is not None:
        cmd.extend(["--tile_width", str(width)])
    if height is not None:
        cmd.extend(["--tile_height", str(height)])

    if depth:
        cmd.extend(["--chunk_depth", str(depth)])
    else:
        cmd.extend(["--downsample-type", "AREA"])

    cmd.extend([fp_in.as_posix(), output_zarr])
    FilePath.run(cmd=cmd, log_file=log_fp)
    return Path(output_zarr)


def bioformats_gen_zarr(
    file_path: FilePath,
    input_fname: str,
    rechunk: bool = False,
    width: int = None,
    height: int = None,
    resolutions: int = None,
    depth: int = None,
):
    """
    Following params alter based on what kind of flow is running...

    :param input_fname: Depending on the flow, input fname might be different
    :param rechunk: Rechunk zarrs if required
    :param width:
    :param height:
    :param resolutions:
    :param depth: These arguments are only used by BRT and SEM flows
    """
    output_zarr = f"{file_path.working_dir}/{file_path.base}.zarr"
    log_fp = f"{file_path.working_dir}/{file_path.base}_as_zarr.log"
    cmd = [
        Config.bioformats2raw,
        f"--max_workers={BIOFORMATS_NUM_WORKERS}",
        "--overwrite",
        "--compression",
        "blosc",
        "--compression-properties",
        "cname=zstd",
        "--compression-properties",
        "clevel=5",
        "--compression-properties",
        "shuffle=1",
    ]
    if resolutions is not None:
        cmd.extend(["--resolutions", str(resolutions)])
    if width is not None:
        cmd.extend(["--tile_width", str(width)])
    if height is not None:
        cmd.extend(["--tile_height", str(height)])

    if depth:
        cmd.extend(["--chunk_depth", str(depth)])
    else:
        cmd.extend(["--downsample-type", "AREA"])

    cmd.extend([input_fname, output_zarr])
    FilePath.run(cmd=cmd, log_file=log_fp)


def zarr_build_multiscales(file_path: FilePath) -> None:
    zarr = Path(f"{file_path.assets_dir}/{file_path.base}.zarr/0")
    log_file = f"{file_path.working_dir}/{file_path.base}.log"

    utils.log("Building multiscales...")
    cmd_ms = ["zarr_build_multiscales", zarr.as_posix()]
    FilePath.run(cmd=cmd_ms, log_file=log_file)
