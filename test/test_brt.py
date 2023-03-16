import pytest
import shutil
import sys
import os
import platform
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

    def _mock_test_output_dir() -> str:
        """
        Get partial results based on machine type. Alternately could use a HOME env. variable.
        :return: The full path where the previously-created output files are stored
        """
        if platform.system() == 'Linux':
            return "/home/macmenaminpe/code/image_portal_workflows/test/input_files/brt_outputs/"
        elif platform.system() == 'Darwin':
            return "/Users/mbopf/projects/hedwig/data/brt_outputs/"

    def _cmd_loc(cmd: str) -> str:
        """
        Given the name of a program that is assumed to be in the current path,
        return the full path by using the `shutil.which()` operation. It is
        *assumed* that `shutil` is available and the command is on the path
        :param cmd: str, the command to be run, often part of the `IMOD` package
        :return: str, the full path to the program
        """
        cmd_path = shutil.which(cmd)
        return cmd_path

    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "mount_point", f"{os.getcwd()}")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp/")

    monkeypatch.setattr(Config, "binvol", _cmd_loc("binvol"))
    monkeypatch.setattr(Config, "brt_binary", _cmd_loc("batchruntomo"))
    monkeypatch.setattr(Config, "clip_loc", _cmd_loc("clip"))
    monkeypatch.setattr(Config, "dm2mrc_loc", _cmd_loc("dm2mrc"))
    monkeypatch.setattr(Config, "header_loc", _cmd_loc("header"))
    monkeypatch.setattr(Config, "mrc2tif_loc", _cmd_loc("mrc2tif"))
    monkeypatch.setattr(Config, "newstack_loc", _cmd_loc("newstack"))
    monkeypatch.setattr(Config, "tif2mrc_loc", _cmd_loc("tif2mrc"))
    monkeypatch.setattr(Config, "xfalign_loc", _cmd_loc("xfalign"))
    monkeypatch.setattr(Config, "xftoxg_loc", _cmd_loc("xftoxg"))

    monkeypatch.setattr(Config, "convert_loc", "convert")

    @task
    def _prep_mock_brt_run(
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
        shutil.copy(os.path.join(_mock_test_output_dir(), "2013-1220-dA30_5-BSC-1_10_ali.mrc"), file_path.working_dir)
        shutil.copy(os.path.join(_mock_test_output_dir(), "2013-1220-dA30_5-BSC-1_10_rec.mrc"), file_path.working_dir)

    @task
    def _create_brt_command(adoc_fp: Path) -> str:
        s = f"cp {_mock_test_output_dir()}*.mrc {adoc_fp.parent}"
        prefect.context.get("logger").info(f"MOCKED {s}")
        return s

    monkeypatch.setattr(utils, 'run_brt', _prep_mock_brt_run)


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
        no_api=True
    )
    assert result.is_successful()
