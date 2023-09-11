from os import cpu_count

VALID_CZI_INPUTS = ["czi", "CZI"]
RECHUNK_SIZE = 512
SITK_COMPRESSION_LVL = 90
THUMB_X_DIM = 300
THUMB_Y_DUM = 300

# at 20 we're seeing drop off in perf increases on HPC, tune this over time.
if cpu_count() is None:
    BIOFORMATS_NUM_WORKERS = 1
elif cpu_count() - 2 > 20:
    BIOFORMATS_NUM_WORKERS = 20
else:
    BIOFORMATS_NUM_WORKERS = cpu_count() - 2
