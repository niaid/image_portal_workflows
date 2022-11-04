import pytest
import shutil
import sys
import os
import prefect
from prefect import task
from pathlib import Path
from prefect.executors import LocalExecutor

from em_workflows.config import Config
from em_workflows.utils import utils
from em_workflows.file_path import FilePath

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
    monkeypatch.setattr(Config, "brt_binary", "/usr/local/IMOD/bin/batchruntomo")
    monkeypatch.setattr(Config, "dm2mrc_loc", "/usr/local/IMOD/bin/dm2mrc")
    monkeypatch.setattr(Config, "mrc2tif_loc" , "/usr/local/IMOD/bin/mrc2tif")
    monkeypatch.setattr(Config,"tif2mrc_loc" , "/usr/local/IMOD/bin/tif2mrc")
    monkeypatch.setattr(Config,"xfalign_loc" , "/usr/local/IMOD/bin/xfalign")
    monkeypatch.setattr(Config,"xftoxg_loc" , "/usr/local/IMOD/bin/xftoxg")
    monkeypatch.setattr(Config,"newstack_loc" , "/usr/local/IMOD/bin/newstack")
    monkeypatch.setattr(Config,"header_loc" , "/usr/local/IMOD/bin/header")
    monkeypatch.setattr(Config,"convert_loc" , "/usr/bin/convert")
    monkeypatch.setattr(Config,"clip_loc" , "/usr/local/IMOD/bin/clip")
    monkeypatch.setattr(Config,"binvol" , "/usr/local/IMOD/bin/binvol")

    @task
    def _run_brt(
            file_path: FilePath,
            adoc_template: str,
            montage: int,
            gold: int,
            focus: int,
            fiducialless: int,
            trackingMethod: int,
            TwoSurfaces: int,
            TargetNumberOfBeads: int,
            LocalAlignments: int,
            THICKNESS: int,
            ) -> None:
        prefect.context.get("logger").info(f"MOCKED {file_path}")
        shutil.copy("/home/macmenaminpe/code/image_portal_workflows/test/input_files/brt_outputs/2013-1220-dA30_5-BSC-1_10_ali.mrc", file_path.working_dir)
        shutil.copy("/home/macmenaminpe/code/image_portal_workflows/test/input_files/brt_outputs/2013-1220-dA30_5-BSC-1_10_rec.mrc", file_path.working_dir)

    @task
    def _create_brt_command(adoc_fp: Path) -> str:
        s = f"cp  /home/macmenaminpe/code/image_portal_workflows/test/input_files/brt_outputs/*.mrc {adoc_fp.parent}"
        prefect.context.get("logger").info(f"MOCKED {s}")
        return s

    monkeypatch.setattr(utils,'run_brt', _run_brt)


def test_brt(hpc_env):
    from em_workflows.brt.flow import flow

    result = flow.run(
        adoc_template="plastic_brt",
        montage=0,
        gold=15,
        focus=0,
        fiducialless=1,
        trackingMethod=None,
        TwoSurfaces=0,
        TargetNumberOfBeads=20,
        LocalAlignments=0,
        THICKNESS=30,
        input_dir="test/input_files/brt_inputs/Projects/",
        token="the_token",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()
