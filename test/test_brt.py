from typing import List
import pytest
import sys
import tempfile
import os
import prefect
from prefect import task
from pathlib import Path
from prefect.executors import LocalExecutor

sys.path.append("..")

from image_portal_workflows.config import Config
from image_portal_workflows.utils import utils

# from image_portal_workflows.brt import flow


@pytest.fixture
def hpc_env(monkeypatch):
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "proj_dir", f"{os.getcwd()}/")
    monkeypatch.setattr(Config, "assets_dir", os.getcwd())
    monkeypatch.setattr(Config, "mount_point", f"{os.getcwd()}")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp/")

    @task
    def _create_brt_command(adoc_fp: Path) -> str:
        s = f"cp {Config.proj_dir}test/input_files/brt_outputs/*.mrc {adoc_fp.parent}"
        prefect.context.get("logger").info(f"MOCKED {s}")
        return s

    monkeypatch.setattr(utils, "create_brt_command", _create_brt_command)


def test_dm4_conv(hpc_env):
    from image_portal_workflows.brt.flow import f

    result = f.run(
        adoc_template="dirTemplate",
        # dual="0",
        montage="0",
        gold="15",
        focus="0",
        bfocus="0",
        fiducialless="1",
        trackingMethod=None,
        TwoSurfaces="0",
        TargetNumberOfBeads="20",
        LocalAlignments="0",
        THICKNESS="30",
        input_dir="test/input_files/brt_inputs/Projects",
        token="the_token",
        sample_id="the_sample_id",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()
