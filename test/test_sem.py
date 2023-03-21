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
        #return "/home"

    def _cmd_loc(cmd: str) -> str:
        """
        Given the name of a program that is assumed to be in the current path,
        return the full path by using the `shutil.which()` operation. It is
        *assumed* that `shutil` is available and the command is on the path
        :param cmd: the command to be run, often part of the `IMOD` package
        :return: the full path to the program
        """
        cmd_path = shutil.which(cmd)
        return cmd_path

    monkeypatch.setattr(Config, "mount_point", os.getcwd() + "/test/input_files")
    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "tmp_dir", "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "tif2mrc_loc", "/usr/local/IMOD/bin/tif2mrc")
    monkeypatch.setattr(Config, "xfalign_loc", "/usr/local/IMOD/bin/xfalign")
    monkeypatch.setattr(Config, "xftoxg_loc", "/usr/local/IMOD/bin/xftoxg")
    monkeypatch.setattr(Config, "newstack_loc", "/usr/local/IMOD/bin/newstack")
    monkeypatch.setattr(Config, "mrc2tif_loc", "/usr/local/IMOD/bin/mrc2tif")


    monkeypatch.setattr(Config, "mrc2tif_loc", _cmd_loc("mrc2tif"))
    monkeypatch.setattr(Config, "newstack_loc", _cmd_loc("newstack"))
    monkeypatch.setattr(Config, "tif2mrc_loc", _cmd_loc("tif2mrc"))
    monkeypatch.setattr(Config, "xfalign_loc", _cmd_loc("xfalign"))
    monkeypatch.setattr(Config, "xftoxg_loc", _cmd_loc("xftoxg"))

    monkeypatch.setattr(Config, "convert_loc", _cmd_loc("convert"))


def test_sem(mock_nfs_mount):
    from em_workflows.sem_tomo.flow import flow

    result = flow.run(
        input_dir="/test/input_files/sem_inputs/Projects/",
        tilt_angle="30.2",
        no_api=True
    )
    assert result.is_successful()
