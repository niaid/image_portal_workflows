import pytest
import sys
from prefect.executors import LocalExecutor

sys.path.append("..")
import os


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    from image_portal_workflows.config import Config

    monkeypatch.setattr(Config, "mount_point", os.getcwd() + "/test/input_files")
    monkeypatch.setattr(Config, "proj_dir", os.getcwd())
    monkeypatch.setattr(Config, "assets_dir", os.getcwd())
    monkeypatch.setattr(Config, "tmp_dir", os.getcwd() + "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "xfalign_loc", "/usr/local/IMOD/bin/xfalign")
    monkeypatch.setattr(Config, "tif2mrc_loc", "/usr/local/IMOD/bin/tif2mrc")
    monkeypatch.setattr(Config, "mrc2tif_loc", "/usr/local/IMOD/bin/mrc2tif")
    monkeypatch.setattr(Config, "xftoxg_loc", "/usr/local/IMOD/bin/xftoxg")
    monkeypatch.setattr(Config, "newstack_loc", "/usr/local/IMOD/bin/newstack")
    monkeypatch.setattr(Config, "convert_loc", "/usr/bin/convert")

    # from image_portal_workflows.utils.utils import _add_outputs, _gen_callback_file_list


def test_sem(mock_nfs_mount):
    from image_portal_workflows.sem_tomo.flow import flow

    result = flow.run(
        input_dir="/test/input_files/sem_inputs/Projects/",
        tilt_parameter="30.2",
        token="the_token",
        environment="dev",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()
