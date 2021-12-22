import pytest
import sys

from prefect.engine import signals
from prefect.tasks.prefect import create_flow_run

sys.path.append("..")
from image_portal_workflows.config import Config
from image_portal_workflows.dm_conversion.flow import flow
import os


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    monkeypatch.setattr(Config, "proj_dir", "")


def test_dm4_conv(mock_nfs_mount):
    result = flow.run(
        input_dir=os.getcwd() + "/test/input_files/",
    )
    assert result.is_successful()


def test_input_fname(mock_nfs_mount):
    flow.run(
        input_dir=os.getcwd() + "/test/input_files/",
        file_name="20210525_1416_A000_G000.dm4",
    )


def test_single_file_not_found_gens_exception(mock_nfs_mount):
    state = flow.run(
        input_dir=os.getcwd() + "/test/input_files/", file_name="does_not_exist"
    )
    assert state.is_failed()
