import os

from em_workflows.config import Config


class SEMConfig(Config):
    convert_loc = os.environ.get(
        "CONVERT_LOC", "/usr/bin/convert"
    )  # requires imagemagick
    imod_root = "/gs1/apps/user/spack-0.16.0/spack/opt/spack/linux-centos7-sandybridge/gcc-8.3.1/imod-4.12.47-2fcggru32s3f4jl3ar5m2rztuqz5h2or"
    tif2mrc_loc = os.environ.get("TIF2MRC_LOC", f"{imod_root}/bin/tif2mrc")
    xfalign_loc = os.environ.get("XFALIGN_LOC", f"{imod_root}/bin/xfalign")
    xftoxg_loc = os.environ.get("XFTOXG_LOC", f"{imod_root}/bin/xftoxg")
