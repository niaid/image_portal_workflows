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


# SO refers to spatial omics
NFS_MOUNT = {
    "RMLEMHedwigDev": "/mnt/ai-fas12/RMLEMHedwigDev",
    "RMLEMHedwigQA": "/mnt/ai-fas12/RMLEMHedwigQA",
    "RMLEMHedwigProd": "/mnt/ai-fas12/RMLEMHedwigProd",
    "RMLSOHedwigDev": "/mnt/ai-fas12/RMLSOHedwigDev",
    "RMLSOHedwigQA": "/mnt/ai-fas12/RMLSOHedwigQA",
    "RMLSOHedwigProd": "/mnt/ai-fas12/RMLSOHedwigProd",
}
