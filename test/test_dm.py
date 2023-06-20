# test_dm.py
"""
test_dm.py runs a number of 2D pipeline tests

NOTE: These tests depend on setup performed in conftest.py
"""
from em_workflows.config import Config
from em_workflows.utils import utils
import os
from pathlib import Path


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        no_api=True,
        return_state=True,
    )
    assert state.is_completed()


def test_dm4_conv_bad_tif(mock_nfs_mount, caplog):
    """
    Create an invalid TIF file in the input directory which should produce an error
    and a failed run, but verify that the rest of the input files get processed.
    """
    from em_workflows.dm_conversion.flow import dm_flow

    proj_dir = Config.proj_dir(utils.get_environment())
    proj_path = Path(proj_dir)
    input_dir = "test/input_files/dm_inputs/Projects/Lab/PI"
    empty_tif = Path(proj_path / input_dir / "empty.tif")
    # Create empty tif file which will cause error
    open(empty_tif, "a").close()

    state = dm_flow(
        input_dir=input_dir,
        no_api=True,
        return_state=True,
    )
    os.remove(empty_tif)
    assert state.is_failed()
    assert "Cannot read TIFF header" in caplog.text
    # todo: assert <Assets files ("1-As-70-007_LG.jpeg", ..., etc.) exist>


def test_dm4_conv_clean_workdir(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="20210525_1416_A000_G000.dm4",
        no_api=True,
        keep_workdir=True,
        return_state=True,
    )
    assert state.is_completed()


def test_input_fname(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="20210525_1416_A000_G000.dm4",
        no_api=True,
        return_state=True,
    )
    assert state.is_completed()


def test_single_file_no_ext_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="file_with_no_ext",
        no_api=True,
        return_state=True,
    )
    assert state.is_failed()
    assert "not contain anything to process" in state.message


def test_single_file_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="does_not_exist.test",
        no_api=True,
        return_state=True,
    )
    assert state.is_failed()
