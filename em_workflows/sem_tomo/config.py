import os

from em_workflows.config import Config


class SEMConfig(Config):
    convert_loc = os.environ.get(
        "CONVERT_LOC", "/usr/bin/convert"
    )  # requires imagemagick
    imod_root = "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/imod-5.1.1-vyv6iidgdilzyxoqumqmdbyokzi4cdlx/IMOD/"
    tif2mrc_loc = os.environ.get("TIF2MRC_LOC", f"{imod_root}/bin/tif2mrc")
    xfalign_loc = os.environ.get("XFALIGN_LOC", f"{imod_root}/bin/xfalign")
    xftoxg_loc = os.environ.get("XFTOXG_LOC", f"{imod_root}/bin/xftoxg")
