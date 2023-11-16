from em_workflows.constants import LARGE_DIM, SMALL_DIM

LARGE_2D = f"{LARGE_DIM}x{LARGE_DIM}"
SMALL_2D = f"{SMALL_DIM}x{SMALL_DIM}"

# List of 2D extensions we may want to process
TIFS_EXT = [
    "TIF",
    "TIFF",
    "tif",
    "tiff",
]
DMS_EXT = [
    "DM4",
    "DM3",
    "dm4",
    "dm3",
]
JPGS_EXT = [
    "JPEG",
    "JPG",
    "jpeg",
    "jpg",
]
PNGS_EXT = [
    "PNG",
    "png",
]
MRCS_EXT = [
    "mrc",
    "MRC",
]
VALID_2D_INPUT_EXTS = TIFS_EXT + DMS_EXT + JPGS_EXT + PNGS_EXT + MRCS_EXT
KEYIMG_EXTS = TIFS_EXT + JPGS_EXT + PNGS_EXT
