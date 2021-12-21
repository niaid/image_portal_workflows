import pytest
import sys

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
