"""
conftest.py is a local, per-directory plugin for Pytest. All fixtures in this file
will be made available to all tests within this directory and do not need to be imported
in the test files.
"""
import os
from pathlib import Path
import shutil

import pytest
from prefect.task_runners import ConcurrentTaskRunner
from prefect.testing.utilities import prefect_test_harness

from em_workflows.file_path import FilePath


@pytest.fixture
def mock_binaries(monkeypatch):
    from em_workflows.config import Config

    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", ConcurrentTaskRunner())
    monkeypatch.setattr(Config, "HIGH_SLURM_EXECUTOR", ConcurrentTaskRunner())


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
        if isinstance(data, dict) and "files" in data:
            # We are only going to dump data into temporary file, that we 'know' are generated by us.
            with open(filename, "w") as outfile:
                json_data = real_dumps(data)
                outfile.write(json_data)
        return real_dumps(data, *a, **kw)

    monkeypatch.setattr(utils_json, "dumps", _mock_dumps)
    return str(filename)


@pytest.fixture
def mock_reuse_zarr(monkeypatch):
    """
    Reuses zarr generated from bioformats2raw
    One of the most expensive operation in tests is using bf2raw command
    This test assumes that we have already ran the test once. If the .zarr
    converted file is found, reuses the file without re-executing bf2raw command

    NOTE:
        If test files change, zarr steps might show random errors
        Stop using this monkeypatch in such scenario
    """
    from em_workflows.utils import neuroglancer as ng

    original_gen_zarr = ng.bioformats_gen_zarr
    original_rechunk = ng.rechunk_zarr

    def _mock_bioformats_gen_zarr(file_path: FilePath, *a, **kw):
        zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
        if Path(zarr_fp).exists():
            print("Reusing existing .zarr files! Avoiding bf2raw command.")
            return
        original_gen_zarr(file_path, *a, **kw)

    def _mock_rechunk(file_path: FilePath, *a, **kw):
        zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
        work_fp = f"{file_path.working_dir}/{file_path.base}.zarr"
        if Path(zarr_fp).exists():
            shutil.copytree(zarr_fp, work_fp)
            print("No need to rechunk!")
            return
        original_rechunk(file_path, *a, **kw)

    monkeypatch.setattr(ng, "bioformats_gen_zarr", _mock_bioformats_gen_zarr)
    monkeypatch.setattr(ng, "rechunk_zarr", _mock_rechunk)


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """
    Reference: https://docs.prefect.io/2.13.5/guides/testing/
    """
    with prefect_test_harness():
        yield


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
