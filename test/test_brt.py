# test_brt.py
"""
test_brt.py runs an end-to-end test of the batchruntomo pipeline

NOTE: These tests depend on setup performed in conftest.py
"""
import pytest
import json
from pathlib import Path


@pytest.mark.localdata
@pytest.mark.slow
def test_brt_p(mock_nfs_mount):
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
def test_brt_callback(mock_nfs_mount, caplog, mock_callback_data):
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

    callback_output = {}
    with open(mock_callback_data) as fd:
        callback_output = json.load(fd)

    assert "files" in callback_output
