import os

from em_workflows.config import Config


class DMConfig(Config):
    dm2mrc_loc = os.environ.get("DM2MRC_LOC", "/opt/rml/imod/bin/dm2mrc")
