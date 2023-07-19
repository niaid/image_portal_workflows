# test_brt.py
"""
test_brt.py runs an end-to-end test of the batchruntomo pipeline

NOTE: These tests depend on setup performed in conftest.py
"""
import pytest


@pytest.mark.localdata
@pytest.mark.slow
def test_brt(mock_nfs_mount):
    from em_workflows.brt.flow import flow

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
        input_dir="test/input_files/brt_inputs/Projects/",
        no_api=True,
        keep_workdir=True,
    )
    assert result.is_successful()
