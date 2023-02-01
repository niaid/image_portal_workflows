from typing import Dict, List
from em_workflows.utils import utils
import pytest
from prefect.executors import LocalExecutor
import os


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    from em_workflows.config import Config


    def _mock_proj_dir(env: str) -> str:
        return os.getcwd()

    def _mock_assets_dir(env: str) -> str:
        return "/home"

#     monkeypatch.setattr(utils, "send_callback_body", _mock_send_callback)
    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "mount_point", os.getcwd() + "/test/input_files")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "header_loc", "/usr/local/IMOD/bin/header")
    monkeypatch.setattr(Config, "dm2mrc_loc", "/usr/local/IMOD/bin/dm2mrc")
    monkeypatch.setattr(Config, "mrc2tif_loc", "/usr/local/IMOD/bin/mrc2tif")


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        no_api=True,
    )
    assert state.is_successful()


def test_input_fname(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="20210525_1416_A000_G000.dm4",
        no_api=True,
    )
    assert state.is_successful()


def test_single_file_no_ext_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="file_with_no_ext",
        no_api=True,
    )
    assert state.is_failed()


def test_single_file_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="does_not_exist.test",
        no_api=True,
    )
    assert state.is_failed()
