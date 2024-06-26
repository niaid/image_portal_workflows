"""
test_dm.py runs a number of 2D pipeline tests

NOTE: These tests depend on setup performed in conftest.py
"""
import json
import pytest
from unittest.mock import patch


def test_dm4_conv(mock_nfs_mount):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed(), f"State is {state}"


def test_dm4_pipeline_server_response_structure(mock_nfs_mount, mock_callback_data):
    from em_workflows.dm_conversion.flow import dm_flow

    state = dm_flow(
        file_share="test",
        input_dir="/test/input_files/dm_inputs/Projects/Lab/PI",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed(), f"State is {state}"

    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response
    assert isinstance(response["files"], list)
    results = response["files"]

    expected_keys = sorted(
        "primaryFilePath status message thumbnailIndex title fileMetadata imageSet".split()
    )
    for result in results:
        assert expected_keys == sorted(list(result.keys()))

    assert all([result["status"] == "success" for result in results])

    expected_asset_types = ["keyImage", "thumbnail"]
    expected_imageset_keys = sorted("assets imageName imageMetadata".split())
    for result in results:
        assert len(result["imageSet"]) == 1
        imageset = result["imageSet"][0]
        assets = imageset["assets"]
        assert sorted(imageset.keys()) == expected_imageset_keys
        obtained_asset_types = [asset["type"] for asset in assets]
        assert sorted(obtained_asset_types) == expected_asset_types
        assert all(["path" in asset for asset in assets])

    # Remove response exact match if test files change is frequent
    expected_response = {"files": [{"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/1-As-70-007.tif", "status": "success", "message": None, "thumbnailIndex": 0, "title": "1-As-70-007", "fileMetadata": None, "imageSet": [{"imageName": "1-As-70-007", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/1-As-70-007/1-As-70-007_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/1-As-70-007/1-As-70-007_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/WT-2hr_008.tif", "status": "success", "message": None, "thumbnailIndex": 0, "title": "WT-2hr_008", "fileMetadata": None, "imageSet": [{"imageName": "WT-2hr_008", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/WT-2hr_008/WT-2hr_008_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/WT-2hr_008/WT-2hr_008_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/PrP-Protein.007.tif", "status": "success", "message": None, "thumbnailIndex": 0, "title": "PrP-Protein.007", "fileMetadata": None, "imageSet": [{"imageName": "PrP-Protein.007", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/PrP-Protein.007/PrP-Protein.007_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/PrP-Protein.007/PrP-Protein.007_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/20210525_1416.dm4", "status": "success", "message": None, "thumbnailIndex": 0, "title": "20210525_1416", "fileMetadata": None, "imageSet": [{"imageName": "20210525_1416", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416/20210525_1416_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416/20210525_1416_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/20210525_1416_A000_G000.dm4", "status": "success", "message": None, "thumbnailIndex": 0, "title": "20210525_1416_A000_G000", "fileMetadata": None, "imageSet": [{"imageName": "20210525_1416_A000_G000", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416_A000_G000/20210525_1416_A000_G000_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416_A000_G000/20210525_1416_A000_G000_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/20210525_1416_A000_G000(H019).dm4", "status": "success", "message": None, "thumbnailIndex": 0, "title": "20210525_1416_A000_G000(H019)", "fileMetadata": None, "imageSet": [{"imageName": "20210525_1416_A000_G000(H019)", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416_A000_G000(H019)/20210525_1416_A000_G000(H019)_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/20210525_1416_A000_G000(H019)/20210525_1416_A000_G000(H019)_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/Con1E1-ApoA1-54mAu-3.jpg", "status": "success", "message": None, "thumbnailIndex": 0, "title": "Con1E1-ApoA1-54mAu-3", "fileMetadata": None, "imageSet": [{"imageName": "Con1E1-ApoA1-54mAu-3", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/Con1E1-ApoA1-54mAu-3/Con1E1-ApoA1-54mAu-3_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/Con1E1-ApoA1-54mAu-3/Con1E1-ApoA1-54mAu-3_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/Con1E1-ApoA1-54mAu-1.jpg", "status": "success", "message": None, "thumbnailIndex": 0, "title": "Con1E1-ApoA1-54mAu-1", "fileMetadata": None, "imageSet": [{"imageName": "Con1E1-ApoA1-54mAu-1", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/Con1E1-ApoA1-54mAu-1/Con1E1-ApoA1-54mAu-1_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/Con1E1-ApoA1-54mAu-1/Con1E1-ApoA1-54mAu-1_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/P6_J131_real_space_slices_iteration_008.png", "status": "success", "message": None, "thumbnailIndex": 0, "title": "P6_J131_real_space_slices_iteration_008", "fileMetadata": None, "imageSet": [{"imageName": "P6_J131_real_space_slices_iteration_008", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/P6_J131_real_space_slices_iteration_008/P6_J131_real_space_slices_iteration_008_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/P6_J131_real_space_slices_iteration_008/P6_J131_real_space_slices_iteration_008_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/P6_J130_fsc_iteration_001.png", "status": "success", "message": None, "thumbnailIndex": 0, "title": "P6_J130_fsc_iteration_001", "fileMetadata": None, "imageSet": [{"imageName": "P6_J130_fsc_iteration_001", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/P6_J130_fsc_iteration_001/P6_J130_fsc_iteration_001_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/P6_J130_fsc_iteration_001/P6_J130_fsc_iteration_001_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/P6_J128_selected_11(classes).png", "status": "success", "message": None, "thumbnailIndex": 0, "title": "P6_J128_selected_11(classes)", "fileMetadata": None, "imageSet": [{"imageName": "P6_J128_selected_11(classes)", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/P6_J128_selected_11(classes)/P6_J128_selected_11(classes)_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/P6_J128_selected_11(classes)/P6_J128_selected_11(classes)_LG.jpeg"}]}]}, {"primaryFilePath": "test/input_files/dm_inputs/Projects/Lab/PI/SARsCoV2_1.mrc", "status": "success", "message": None, "thumbnailIndex": 0, "title": "SARsCoV2_1", "fileMetadata": None, "imageSet": [{"imageName": "SARsCoV2_1", "imageMetadata": None, "assets": [{"type": "thumbnail", "path": "test/input_files/dm_inputs/Assets/Lab/PI/SARsCoV2_1/SARsCoV2_1_SM.jpeg"}, {"type": "keyImage", "path": "test/input_files/dm_inputs/Assets/Lab/PI/SARsCoV2_1/SARsCoV2_1_LG.jpeg"}]}]}]}  # noqa
    # assert response == expected_response


def test_dm4_pipeline_partial_fail_server_response(mock_nfs_mount, mock_callback_data):
    pytest.skip("Missing failing data")


def test_dm4_pipeline_failure_server_response(mock_nfs_mount, mock_callback_data):
    pytest.skip("Missing failing data")


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
