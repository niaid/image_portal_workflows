from typing import Dict, List
from em_workflows.utils import utils
import pytest
from prefect.executors import LocalExecutor
import os
import shutil


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    from em_workflows.config import Config

    def _mock_proj_dir(env: str) -> str:
        return os.getcwd()

    def _mock_assets_dir(env: str) -> str:
        return os.getcwd()

    def _cmd_loc(cmd: str) -> str:
        """
        Given the name of a program that is assumed to be in the current path,
        return the full path by using the `shutil.which()` operation. It is
        *assumed* that `shutil` is available and the command is on the path
        :param cmd: the command to be run, often part of the `IMOD` package
        :return: str, the full path to the program
        """
        cmd_path = shutil.which(cmd)
        return cmd_path


#     monkeypatch.setattr(utils, "send_callback_body", _mock_send_callback)
    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "mount_point", os.getcwd() + "/test/input_files")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "header_loc", _cmd_loc("header"))
    monkeypatch.setattr(Config, "dm2mrc_loc", _cmd_loc("dm2mrc"))
    monkeypatch.setattr(Config, "mrc2tif_loc", _cmd_loc("mrc2tif"))


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