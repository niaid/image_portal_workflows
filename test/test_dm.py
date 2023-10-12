"""
test_dm.py runs a number of 2D pipeline tests

NOTE: These tests depend on setup performed in conftest.py
"""
import pytest
from unittest.mock import patch


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        no_api=True,
    )
    assert state.is_completed()


def test_dm4_conv_clean_workdir(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow
    from em_workflows.file_path import FilePath

    with patch.object(FilePath, "rm_workdir") as patch_rm:
        state = dm_flow(
            file_share="test",
            input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
            file_name="20210525_1416_A000_G000.dm4",
            no_api=True,
            keep_workdir=False,
        )
        assert state.is_completed()
    # keep_workdir = False removes the workdir
    patch_rm.assert_called()

    with patch.object(FilePath, "rm_workdir") as patch_rm:
        state = dm_flow(
            file_share="test",
            input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
            file_name="20210525_1416_A000_G000.dm4",
            no_api=True,
            keep_workdir=True,
        )
        assert state.is_completed()
    # keep_workdir keeps the workdir
    patch_rm.assert_not_called()


@pytest.mark.skip(reason="Skipping: strange failure error")
def test_single_file_no_ext_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="file_with_no_ext",
        no_api=True,
    )
    assert state.is_failed()


@pytest.mark.skip(reason="Skipping: strange failure error")
def test_single_file_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="does_not_exist.test",
        no_api=True,
    )
    assert state.is_failed()
