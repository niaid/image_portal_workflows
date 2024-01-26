"""
test_dm.py runs a number of 2D pipeline tests

NOTE: These tests depend on setup performed in conftest.py
"""
from unittest.mock import patch


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed()


def test_dm4_conv_clean_workdir(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow
    from em_workflows.file_path import FilePath

    with patch.object(FilePath, "rm_workdir") as patch_rm:
        state = dm_flow(
            file_share="test",
            input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
            x_file_name="20210525_1416_A000_G000.dm4",
            x_no_api=True,
            x_keep_workdir=False,
            return_state=True,
        )
        assert state.is_completed()
    # x_keep_workdir = False removes the workdir
    patch_rm.assert_called()

    with patch.object(FilePath, "rm_workdir") as patch_rm:
        state = dm_flow(
            file_share="test",
            input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
            x_file_name="20210525_1416_A000_G000.dm4",
            x_no_api=True,
            x_keep_workdir=True,
            return_state=True,
        )
        assert state.is_completed()
    # x_keep_workdir keeps the workdir
    patch_rm.assert_not_called()


def test_single_file_no_ext_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        x_file_name="file_with_no_ext",
        x_no_api=True,
        x_keep_workdir=True,
        return_state=True,
    )
    assert state.is_failed()


def test_single_file_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        x_file_name="does_not_exist.test",
        x_no_api=True,
        x_keep_workdir=True,
        return_state=True,
    )
    assert state.is_failed()
