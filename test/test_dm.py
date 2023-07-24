# test_dm.py
"""
test_dm.py runs a number of 2D pipeline tests

NOTE: These tests depend on setup performed in conftest.py
"""
import pytest
from em_workflows.config import Config
from em_workflows.utils import utils
import os
from pathlib import Path


def create_inp_path(input_str: str) -> Path:
    """
    Convenience function to create Path pointing to local input FilePath structure
    :param input_str: string relative to proj_dir; do not include initial '/'
    :return: pathlib.Path for input_str
    """
    if input_str.startswith("/"):
        input_str = input_str[1:]  # pathlib interprets leading '/' as an absolute path
    return Path(Config.proj_dir(utils.get_environment())) / input_str


# Commonly used functions @todo generalize and promote to test utilities module?
def delete_in_proj_dir(del_dir: str, recreate: bool = False):
    """
    :param recreate:
    :param del_dir: Directory to be deleted
    This removes a directory in the input_files path, usually to remove the results
    of a previous test.
    """
    from shutil import rmtree

    input_path = create_inp_path(del_dir)
    # Remove old directory
    if "input_files" not in str(input_path):  # Safety check to prevent invalid deletion
        raise ValueError(f"Path '{input_path}' must contain 'input_files'")
    elif not input_path.is_dir():
        raise ValueError(f"Path '{input_path}' must be a directory")
    else:
        rmtree(input_path)

    if recreate:
        os.mkdir(input_path)


# Current counts of valid JPEGS produced in valid test; may need to change this
# if the test or code changes.
FULL_JPEG_CNT = 24


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    """
    Delete previous outputs, and then count the jpegs to verify the test succeeded
    """
    del_dir = "test/input_files/dm_inputs/Assets/Lab/PI"
    asset_path = create_inp_path(del_dir)
    if asset_path.exists():
        delete_in_proj_dir(del_dir, recreate=True)

    state = flow.run(
        input_dir="test/input_files/dm_inputs/Projects/Lab/PI",
        no_api=True,
    )
    assert state.is_successful()

    jpegs = list(asset_path.glob("**/*.jpeg"))
    # This test should create 24 jpegs and put them in the Assets dir
    assert len(jpegs) == FULL_JPEG_CNT


def test_dm4_conv_bad_tif(mock_nfs_mount, caplog):
    """
    Create an invalid TIF file in the input directory which should produce an error
    and a failed run, but verify that the rest of the input files get processed.
    """
    from em_workflows.dm_conversion.flow import flow

    del_dir = "test/input_files/dm_inputs/Assets/Lab/PI"
    asset_path = create_inp_path(del_dir)
    if asset_path.exists():
        delete_in_proj_dir(del_dir, recreate=False)

    input_dir = "test/input_files/dm_inputs/Projects/Lab/PI"
    empty_tif = create_inp_path(input_dir + "/empty.tif")
    # Create empty tif file which will cause error
    open(empty_tif, "a").close()

    state = flow.run(  # noqa: F841
        input_dir=input_dir,
        no_api=True,
    )

    os.remove(empty_tif)
    delete_in_proj_dir(del_dir + "/empty")  # clean up test-created directory

    #    assert state.is_failed()  # Not working in Prefect 1
    assert "Unable to determine ImageIO reader" in caplog.text
    jpegs = list(asset_path.glob("**/*.jpeg"))
    # This test should create 24 jpegs and put them in the Assets dir
    assert len(jpegs) == FULL_JPEG_CNT


def test_dm4_conv_clean_workdir(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        no_api=True,
        # keep_workdir=True
    )
    assert state.is_successful()


def test_input_fname(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    del_dir = "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416_A000_G000"
    asset_path = create_inp_path(del_dir)
    if asset_path.exists():
        delete_in_proj_dir(del_dir)

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="20210525_1416_A000_G000.dm4",
        no_api=True,
    )
    assert state.is_successful()
    assert Path(asset_path / "20210525_1416_A000_G000_LG.jpeg").exists()
    assert Path(asset_path / "20210525_1416_A000_G000_SM.jpeg").exists()


@pytest.mark.skip(reason="Skipping: strange failure error")
def test_single_file_no_ext_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="file_with_no_ext",
        no_api=True,
    )
    assert state.is_failed()  # This doesn't work consistently in Prefect 1


@pytest.mark.skip(reason="Skipping: strange failure error")
def test_single_file_not_found_gens_exception(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import flow

    state = flow.run(
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        file_name="does_not_exist.test",
        no_api=True,
    )
    assert state.is_failed()  # This doesn't work consistently in Prefect 1
