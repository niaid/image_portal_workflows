import os

from em_workflows.config import Config


class SEMConfig(Config):
    convert_loc = os.environ.get(
        "CONVERT_LOC", "/usr/bin/convert"
    )  # requires imagemagick
    tif2mrc_loc = os.environ.get("TIF2MRC_LOC", "tif2mrc")
    xfalign_loc = os.environ.get("XFALIGN_LOC", "xfalign")
    xftoxg_loc = os.environ.get("XFTOXG_LOC", "xftoxg")
