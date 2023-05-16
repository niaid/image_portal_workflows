# test/conftest.py
"""
conftest.py is a local, per-directory plugin for Pytest. All fixtures in this file
will be made available to all tests within this directory and do not need to be imported
in the test files.
"""
import pytest
import os
import platform
from prefect.executors import LocalExecutor
from em_workflows.config import command_loc


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    from em_workflows.config import Config

    def _mock_proj_dir(env: str) -> str:
        return os.getcwd()

    def _mock_assets_dir(env: str) -> str:
        return os.getcwd()

    def _mock_bioformats2raw() -> str:
        """
        Force M1 Mac to use conda x86 bioformats2raw installation (rather UGLY)
        :return: The full path where the previously-created output files are stored
        """
        if platform.system() == "Linux":
            command_loc("bioformats2raw")
        elif platform.system() == "Darwin":
            return "/Users/mbopf/miniconda-intel/envs/intel_base/bin/bioformats2raw"

    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "bioformats2raw", _mock_bioformats2raw())
    monkeypatch.setattr(Config, "mount_point", os.getcwd() + "/test/input_files")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())

    monkeypatch.setattr(Config, "binvol", command_loc("binvol"))
    monkeypatch.setattr(Config, "brt_binary", command_loc("batchruntomo"))
    monkeypatch.setattr(Config, "clip_loc", command_loc("clip"))
    monkeypatch.setattr(Config, "dm2mrc_loc", command_loc("dm2mrc"))
    monkeypatch.setattr(Config, "header_loc", command_loc("header"))
    monkeypatch.setattr(Config, "mrc2tif_loc", command_loc("mrc2tif"))
    monkeypatch.setattr(Config, "newstack_loc", command_loc("newstack"))
    monkeypatch.setattr(Config, "tif2mrc_loc", command_loc("tif2mrc"))
    monkeypatch.setattr(Config, "xfalign_loc", command_loc("xfalign"))
    monkeypatch.setattr(Config, "xftoxg_loc", command_loc("xftoxg"))

    monkeypatch.setattr(Config, "convert_loc", command_loc("convert"))
