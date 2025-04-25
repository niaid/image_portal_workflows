import os

from em_workflows.config import Config


class DMConfig(Config):
    dm2mrc_loc = os.environ.get("DM2MRC_LOC", f"{Config.imod_root}/bin/dm2mrc")
