# test/conftest.py
"""
conftest.py is a local, per-directory plugin for Pytest. All fixtures in this file
will be made available to all tests within this directory and do not need to be imported
in the test files.
"""
import os
from pathlib import Path
import pytest
from prefect.executors import LocalExecutor
from em_workflows.config import command_loc


@pytest.fixture
def mock_binaries(monkeypatch):
    from em_workflows.config import Config
    from em_workflows.brt.config import BRTConfig
    from em_workflows.dm_conversion.config import DMConfig
    from em_workflows.sem_tomo.config import SEMConfig

    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())

    monkeypatch.setattr(BRTConfig, "binvol", command_loc("binvol"))
    monkeypatch.setattr(Config, "brt_binary", command_loc("batchruntomo"))
    monkeypatch.setattr(BRTConfig, "clip_loc", command_loc("clip"))
    monkeypatch.setattr(DMConfig, "dm2mrc_loc", command_loc("dm2mrc"))
    monkeypatch.setattr(Config, "header_loc", command_loc("header"))
    monkeypatch.setattr(Config, "mrc2tif_loc", command_loc("mrc2tif"))
    monkeypatch.setattr(Config, "newstack_loc", command_loc("newstack"))
    monkeypatch.setattr(SEMConfig, "tif2mrc_loc", command_loc("tif2mrc"))
    monkeypatch.setattr(SEMConfig, "xfalign_loc", command_loc("xfalign"))
    monkeypatch.setattr(SEMConfig, "xftoxg_loc", command_loc("xftoxg"))
    monkeypatch.setattr(SEMConfig, "convert_loc", command_loc("convert"))
    # monkeypatch.setattr(Config, "bioformats2raw", command_loc("bioformats2raw"))


@pytest.fixture
def mock_nfs_mount(monkeypatch, mock_binaries):
    from em_workflows.config import Config

    def _mock_proj_dir(share_name: str) -> str:
        return os.getcwd()

    def _mock_assets_dir(share_name: str) -> str:
        return os.getcwd()

    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "_mount_point", os.getcwd() + "/test/input_files")


@pytest.fixture
def mock_callback_data(monkeypatch, tmp_path):
    """
    In order to assert that callback is appropriate, the callback output
    is dumped into a temporary file. This file can be json.loaded to get
    the callback output for tests.
    """
    from em_workflows.utils.utils import json as utils_json
    from json import dumps as real_dumps

    filename = Path(tmp_path) / "temporary.json"

    def _mock_dumps(data, *a, **kw):
        with open(filename, "w") as outfile:
            json_data = real_dumps(data)
            outfile.write(json_data)
        return real_dumps(data)

    monkeypatch.setattr(utils_json, "dumps", _mock_dumps)
    return str(filename)
