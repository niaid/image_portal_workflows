import pytest
import sys
import json

from pathlib import Path
from prefect.engine import signals
from prefect.tasks.prefect import create_flow_run
from prefect.executors import LocalExecutor

sys.path.append("..")
import os


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    from em_workflows.config import Config

    def _mock_proj_dir(env: str) -> str:
        return os.getcwd()

    def _mock_assets_dir(env: str) -> str:
        return os.getcwd()

    monkeypatch.setattr(Config, "proj_dir", _mock_proj_dir)
    monkeypatch.setattr(Config, "assets_dir", _mock_assets_dir)
    monkeypatch.setattr(Config, "mount_point", os.getcwd() + "/test/input_files")
    monkeypatch.setattr(Config, "tmp_dir", os.getcwd() + "/tmp")
    monkeypatch.setattr(Config, "SLURM_EXECUTOR", LocalExecutor())
    monkeypatch.setattr(Config, "dm2mrc_loc", "/usr/local/IMOD/bin/dm2mrc")
    monkeypatch.setattr(Config, "mrc2tif_loc", "/usr/local/IMOD/bin/mrc2tif")


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    result = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        token="the_token",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()


def test_input_fname(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    flow.run(
        input_dir="/test/input_files/",
        file_name="20210525_1416_A000_G000.dm4",
        token="the_token",
        callback_url="https://ptsv2.com/t/",
    )


def test_single_file_no_ext_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="file_with_no_ext",
        token="the_token",
        callback_url="https://ptsv2.com/t/",
    )
    assert state.is_failed()


def test_single_file_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="does_not_exist.test",
        token="the_token",
        callback_url="https://ptsv2.com/t/",
    )
    assert state.is_failed()


# def test_inputs_to_outputs():
#     dname = "Lab/PI/Myproject/MySession/Sample1/"
#     input_files = [Path("a.txt"), Path("b.txt"), Path("c.txt")]
#     output_files_a = [Path("a.axt"), Path("b.axt"), Path("c.axt")]
#     output_files_b = [Path("a.bxt"), Path("b.bxt"), Path("c.bxt")]
#     _files = _gen_callback_file_list(dname=dname, inputs=input_files)
#     _files = _add_outputs(
#         dname=dname, files=_files, outputs=output_files_a, _type="type_axt"
#     )
#     _files = _add_outputs(
#         dname=dname, files=_files, outputs=output_files_b, _type="type_bxt"
#     )
#
#     assert len(_files) == 3
#     print(json.dumps(_files))
