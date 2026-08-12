from pathlib import Path
from pytools.HedwigZarrImages import HedwigZarrImages
from em_workflows.config import Config
from em_workflows.file_path import FileContext
from em_workflows.constants import BIOFORMATS_NUM_WORKERS, RECHUNK_SIZE
from em_workflows.utils import utils
from em_workflows.config import setup_pytools_log

setup_pytools_log()


def rechunk_zarr(file_path: FileContext) -> None:
    zarr_fp = file_path.working_dir / f"{file_path.base}.zarr"
    utils.log(f"{zarr_fp} output zarr")
    images = HedwigZarrImages(zarr_fp, read_only=False)
    for _, image in images.series():
        image.rechunk(RECHUNK_SIZE, in_memory=True)


def bioformats_gen_zarr(
    fp_in: Path,
    output_dir: Path,
    zarr_stem: str = None,
    width: int = None,
    height: int = None,
    resolutions: int = None,
    depth: int = None,
) -> Path:
    # zarr_stem defaults to fp_in.stem; pass explicitly when input file stem differs from desired zarr name
    zarr_stem = zarr_stem or fp_in.stem
    output_zarr = output_dir / f"{zarr_stem}.zarr"
    log_fp = str(output_dir / f"{zarr_stem}_as_zarr.log")
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
    cmd.extend([fp_in.as_posix(), output_zarr.as_posix()])
    utils.run(
        cmd=cmd,
        log_file=log_fp,
        env={
            "JAVA_OPTS": Config.java_opts,
            "JAVA_TOOL_OPTIONS": Config.java_tool_options,
        },
    )
    return output_zarr


def zarr_build_multiscales(file_path: FileContext) -> None:
    zarr = file_path.assets_dir / f"{file_path.base}.zarr" / "0"
    log_file = str(file_path.working_dir / f"{file_path.base}.log")

    utils.log("Building multiscales...")
    cmd_ms = ["zarr_build_multiscales", zarr.as_posix()]
    utils.run(cmd=cmd_ms, log_file=log_file)


def zarr_build_multiscales2(zarr_fp: Path) -> None:
    zarr = zarr_fp / "0"
    log_file = str(zarr_fp.parent / f"{zarr_fp.stem}.log")

    utils.log("Building multiscales...")
    cmd_ms = ["zarr_build_multiscales", zarr.as_posix()]
    utils.run(cmd=cmd_ms, log_file=log_file)
