import os
from pathlib import Path

from em_workflows.config import Config


class BRTConfig(Config):
    binvol = os.environ.get("BINVOL_LOC", "binvol")
    clip_loc = os.environ.get("CLIP_LOC", "clip")
    ffmpeg_loc = os.environ.get("FFMPEG_LOC", "ffmpeg")

    @classmethod
    def get_flow_job_script_prologue(cls, current_dir: Path = None) -> list[str]:
        return [
            "module load imod",
            "module load bioformats2raw",
        ]
