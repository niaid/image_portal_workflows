# test_brt.py
"""
test_brt.py runs an end-to-end test of the batchruntomo pipeline

NOTE: These tests depend on setup performed in conftest.py
"""
import os
import json
import pytest
from pathlib import Path


@pytest.mark.localdata
@pytest.mark.slow
def test_brt(mock_nfs_mount):
    from em_workflows.brt.flow import brt_flow

    input_dir = "test/input_files/brt/Projects/RT_TOMO/"
    if not Path(input_dir).exists():
        pytest.skip(f"Directory {input_dir} doesn't exist")

    result = brt_flow(
        adoc_template="plastic_brt",
        montage=0,
        gold=15,
        focus=0,
        fiducialless=1,
        trackingMethod=0,
        TwoSurfaces=0,
        TargetNumberOfBeads=20,
        LocalAlignments=0,
        THICKNESS=30,
        file_share="test",
        input_dir=input_dir,
        x_no_api=True,
        x_keep_workdir=True,
        return_state=True,
    )
    assert result.is_completed(), "`result` is not successful!"


@pytest.mark.localdata
@pytest.mark.slow
def test_brt_callback_one_valid_of_two_inputs(
    mock_nfs_mount, caplog, mock_callback_data
):
    """
    If one of the brt inputs is failing (out of two),
    the callback should still send the response with one valid dictionary
    """
    from em_workflows.brt.flow import brt_flow

    input_dir = Path("test") / "input_files" / "brt" / "Projects" / "RT_TOMO" / "Part"
    if not Path(input_dir).exists():
        pytest.skip(f"Directory {input_dir} doesn't exist")

    valid_mrc, broken_st = (
        "2013-1220-dA30_5-BSC-1_10.mrc",
        "broken-20130412-Vn-poxtomo_8.st",
    )
    assert os.listdir(input_dir) == [
        valid_mrc,
        broken_st,
    ], "One valid and a broken file should have existed."

    result = brt_flow(
        adoc_template="plastic_brt",
        montage=0,
        gold=15,
        focus=0,
        fiducialless=1,
        trackingMethod=0,
        TwoSurfaces=0,
        TargetNumberOfBeads=20,
        LocalAlignments=0,
        THICKNESS=30,
        file_share="test",
        input_dir=input_dir.as_posix(),
        x_no_api=True,
        x_keep_workdir=True,
        return_state=True,
    )
    assert result.is_completed(), "`result` is not successful!"

    callback_output = {}
    with open(mock_callback_data) as fd:
        callback_output = json.load(fd)

    assert "files" in callback_output
    assert len(callback_output["files"]) == 1, "Only one file should have passed"
    assert valid_mrc in callback_output["files"][0]["primaryFilePath"]
