from os import cpu_count
from collections import namedtuple

LARGE_DIM = 1024
SMALL_DIM = 300

# at 20 we're seeing drop off in perf increases on HPC, tune this over time.
if cpu_count() is None:
    BIOFORMATS_NUM_WORKERS = 1
elif cpu_count() - 2 > 20:
    BIOFORMATS_NUM_WORKERS = 20
else:
    BIOFORMATS_NUM_WORKERS = cpu_count() - 2

# Refer to AssetType.yaml in documentation source for reference
ASSET_TYPE = namedtuple(
    "ASSET_TYPE",
    [
        "THUMBNAIL",
        "AVERAGED_VOLUME",
        "KEY_IMAGE",
        "REC_MOVIE",
        "TILT_MOVIE",
        "VOLUME",
        "NEUROGLANCER_PRECOMPUTED",
        "NEUROGLANCER_ZARR",
    ],
)
AssetType = ASSET_TYPE(
    "thumbnail",
    "averagedVolume",
    "keyImage",
    "recMovie",
    "tiltMovie",
    "volume",
    "neuroglancerPrecomputed",
    "neuroglancerZarr",
)
