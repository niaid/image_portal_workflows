# test/conftest.py
"""
conftest.py is a local, per-directory plugin for Pytest. All fixtures in this file
will be made available to all tests within this directory and do not need to be imported
in the test files.
"""
import os
from pathlib import Path
import pytest
from prefect.task_runners import ConcurrentTaskRunner


@pytest.fixture
def mock_binaries(monkeypatch):
    from em_workflows.config import Config

    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", ConcurrentTaskRunner())


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


@pytest.fixture(scope="session", autouse=True)
def check_env_setup(request):
    assert os.path.isfile(
        ".env"
    ), "Make sure you have .env file setup with \
            necessary binary filepaths"

    with open(".env", mode="r") as env_file:
        content = env_file.read()
        assert "BIOFORMATS2RAW_LOC" in content
        assert "BRT_LOC" in content
        assert "HEADER_LOC" in content
        assert "MRC2TIF_LOC" in content
        assert "NEWSTACK_LOC" in content
        assert "DM2MRC_LOC" in content
        assert "BINVOL_LOC" in content
        assert "CLIP_LOC" in content
        assert "CONVERT_LOC" in content
        assert "TIF2MRC_LOC" in content
        assert "XFALIGN_LOC" in content
        assert "XFTOXG_LOC" in content
