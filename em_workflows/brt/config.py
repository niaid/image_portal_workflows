import os

from em_workflows.config import Config


class BRTConfig(Config):
    binvol = os.environ.get("BINVOL_LOC", "/opt/rml/imod/bin/binvol")
    clip_loc = os.environ.get("CLIP_LOC", "/opt/rml/imod/bin/clip")
