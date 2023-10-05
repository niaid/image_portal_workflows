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
def test_brt(mock_nfs_mount):
    from em_workflows.brt.flow import flow

    input_dir = "test/input_files/brt_inputs/Projects/"
    if not Path(input_dir).exists():
        pytest.skip("Directory doesn't exist")

    result = flow.run(
        adoc_template="plastic_brt",
        montage=0,
        gold=15,
        focus=0,
        fiducialless=1,
        trackingMethod=None,
        TwoSurfaces=0,
        TargetNumberOfBeads=20,
        LocalAlignments=0,
        THICKNESS=30,
        file_share="test",
        input_dir=input_dir,
        no_api=True,
        keep_workdir=True,
    )
    assert result.is_successful(), "`result` is not successful!"


@pytest.mark.localdata
@pytest.mark.slow
def test_brt_callback(mock_nfs_mount, mock_callback_data):
    from em_workflows.brt.flow import flow

    input_dir = "test/input_files/brt_inputs/Projects/"
    if not Path(input_dir).exists():
        pytest.skip("Directory doesn't exist")

    result = flow.run(
        adoc_template="plastic_brt",
        montage=0,
        gold=15,
        focus=0,
        fiducialless=1,
        trackingMethod=None,
        TwoSurfaces=0,
        TargetNumberOfBeads=20,
        LocalAlignments=0,
        THICKNESS=30,
        file_share="test",
        input_dir=input_dir,
        no_api=True,
        keep_workdir=True,
    )
    assert result.is_successful(), "`result` is not successful!"

    callback_output = {}
    with open(mock_callback_data) as fd:
        callback_output = json.load(fd)

    assert "files" in callback_output
