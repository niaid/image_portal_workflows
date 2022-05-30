import pytest
import sys
import json

from pathlib import Path
from prefect.engine import signals
from prefect.tasks.prefect import create_flow_run

sys.path.append("..")
from image_portal_workflows.config import Config
from image_portal_workflows.dm_conversion.flow import flow
from image_portal_workflows.utils.utils import _add_outputs, _gen_callback_file_list
import os


@pytest.fixture
def mock_nfs_mount(monkeypatch):
    monkeypatch.setattr(Config, "proj_dir", os.getcwd())
    monkeypatch.setattr(Config, "tmp_dir", os.getcwd() + "/tmp")


def test_dm4_conv(mock_nfs_mount):
    result = flow.run(
        input_dir="/test/input_files/",
        token="the_token",
        sample_id="the_sample_id",
        callback_url="https://ptsv2.com/t/",
    )
    assert result.is_successful()


def test_input_fname(mock_nfs_mount):
    flow.run(
        input_dir="/test/input_files/",
        file_name="20210525_1416_A000_G000.dm4",
        token="the_token",
        sample_id="the_sample_id",
        callback_url="https://ptsv2.com/t/",
    )


def test_single_file_not_found_gens_exception(mock_nfs_mount):
    state = flow.run(
        input_dir="/test/input_files/",
        file_name="does_not_exist",
        token="the_token",
        sample_id="the_sample_id",
        callback_url="https://ptsv2.com/t/",
    )
    assert state.is_failed()


def test_inputs_to_outputs():
    dname = "Lab/PI/Myproject/MySession/Sample1/"
    input_files = [Path("a.txt"), Path("b.txt"), Path("c.txt")]
    output_files_a = [Path("a.axt"), Path("b.axt"), Path("c.axt")]
    output_files_b = [Path("a.bxt"), Path("b.bxt"), Path("c.bxt")]
    _files = _gen_callback_file_list(dname=dname, inputs=input_files)
    _files = _add_outputs(
        dname=dname, files=_files, outputs=output_files_a, _type="type_axt"
    )
    _files = _add_outputs(
        dname=dname, files=_files, outputs=output_files_b, _type="type_bxt"
    )

    assert len(_files) == 3
    print(json.dumps(_files))
