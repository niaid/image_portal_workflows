import pytest
import sys
import os
import prefect
from prefect import task
from pathlib import Path
from prefect.executors import LocalExecutor

from em_workflows.config import Config
from em_workflows.utils import utils


@pytest.fixture
def hpc_env(monkeypatch):
    def _mock_proj_dir(env: str) -> str:
        return os.getcwd()

    def _mock_assets_dir(env: str) -> str:
        return os.getcwd()

    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "mount_point", f"{os.getcwd()}")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp/")

    @task
    def _create_brt_command(adoc_fp: Path) -> str:
        s = f"cp {os.getcwd()}/test/input_files/brt_outputs/*.mrc {adoc_fp.parent}"
        prefect.context.get("logger").info(f"MOCKED {s}")
        return s

    monkeypatch.setattr(utils, "create_brt_command", _create_brt_command)


def test_brt(hpc_env):
    from em_workflows.brt.flow import f

    result = f.run(
        adoc_template="plastic_brt",
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
        input_dir="test/input_files/brt_inputs/Projects/",
        token="the_token",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()
