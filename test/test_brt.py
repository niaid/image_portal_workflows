import pytest
import sys
import tempfile
import os
from pathlib import Path
from prefect.executors import LocalExecutor

sys.path.append("..")

from image_portal_workflows.config import Config


@pytest.fixture
def hpc_env(monkeypatch):
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "proj_dir", os.getcwd())
    monkeypatch.setattr(Config, "assets_dir", tempfile.mkdtemp())


# def test_brt(hpc_env, monkeypatch):
#    def mockreturn(self):
#        return Path(os.getcwd() + "/test/input_files/PubChem-522220-bas-color.wrl")
#
# monkeypatch.setattr(Job, "get_file", mockreturn)


def test_dm4_conv(hpc_env):
    from image_portal_workflows.brt.flow import flow

    result = flow.run(
        input_dir="/test/input_files/",
        token="the_token",
        sample_id="the_sample_id",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()
