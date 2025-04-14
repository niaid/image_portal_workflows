import os

from em_workflows.config import Config


class BRTConfig(Config):
    binvol = os.environ.get("BINVOL_LOC", "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/imod-5.1.1-vyv6iidgdilzyxoqumqmdbyokzi4cdlx/IMOD/bin/binvol")
    clip_loc = os.environ.get("CLIP_LOC", "/data/apps/software/spack/linux-rocky9-x86_64_v3/gcc-11.3.1/imod-5.1.1-vyv6iidgdilzyxoqumqmdbyokzi4cdlx/IMOD/bin/clip")
    ffmpeg_loc = "/data/apps/software/conda/ffmpeg-7.1.1/bin/ffmpeg"
