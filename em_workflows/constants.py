from collections import namedtuple


LARGE_DIM = 1024
SMALL_DIM = 300

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
