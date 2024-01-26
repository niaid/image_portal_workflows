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
def test_brt_server_response(mock_nfs_mount, caplog, mock_callback_data):
    from em_workflows.brt.flow import brt_flow

    input_dir = "test/input_files/brt/Projects/RT_TOMO/"
    if not Path(input_dir).exists():
        pytest.skip(f"Directory {input_dir} doesn't exist")

    state = brt_flow(
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
        x_keep_workdir=False,
        return_state=True,
    )
    assert state.is_completed(), "`result` is not successful!"

    response = {}
    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response
    assert isinstance(response["files"], list)
    results = response["files"]
    expected_keys = sorted(
        "primaryFilePath status message thumbnailIndex title fileMetadata imageSet".split()
    )
    expected_imageset_keys = sorted("imageName imageMetadata assets".split())
    expected_asset_types = sorted(
        "thumbnail keyImage neuroglancerZarr volume averagedVolume recMovie tiltMovie".split()
    )
    for result in results:
        assert expected_keys == sorted(list(result.keys()))
        assert len(result["imageSet"]) == 1
        image_set = result["imageSet"][0]
        assert expected_imageset_keys == sorted(list(image_set.keys()))
        assets = image_set["assets"]
        obtained_asset_types = sorted([asset["type"] for asset in assets])
        assert expected_asset_types == obtained_asset_types
        assert all(["path" in asset for asset in assets])
        asset_paths = [asset["path"] for asset in assets]
        assert len(set(asset_paths)) == len(
            asset_paths
        ), "Asset paths should have been different"
    # Note: There are many details to check further, but for now we are simply asserting
    # entire response comes through. In the future, if the inner details change this
    #  should be moved to assert checks
    # Note REMOVE ME if the test data changes frequenlty enough
    expected_response = {
        "files": [
            {
                "primaryFilePath": "test/input_files/brt/Projects/RT_TOMO/2013-1220-dA30_5-BSC-1_10.mrc",
                "status": "success",
                "message": None,
                "thumbnailIndex": 0,
                "title": "2013-1220-dA30_5-BSC-1_10",
                "fileMetadata": None,
                "imageSet": [
                    {
                        "imageName": "2013-1220-dA30_5-BSC-1_10",
                        "imageMetadata": None,
                        "assets": [
                            {
                                "type": "thumbnail",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/keyimg_2013-1220-dA30_5-BSC-1_10_ali_ali.060_s.jpg",
                            },
                            {
                                "type": "keyImage",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/2013-1220-dA30_5-BSC-1_10_ali_ali.060.jpg",
                            },
                            {
                                "type": "neuroglancerZarr",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/2013-1220-dA30_5-BSC-1_10.zarr/0",
                                "metadata": {
                                    "shader": "Grayscale",
                                    "dimensions": "XYZ",
                                    "shaderParameters": {
                                        "range": [-785, -419],
                                        "window": [-1242, -278],
                                    },
                                },
                            },
                            {
                                "type": "volume",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/ave_2013-1220-dA30_5-BSC-1_10_rec.mrc",
                            },
                            {
                                "type": "averagedVolume",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/avebin8_ave_2013-1220-dA30_5-BSC-1_10_rec.mrc",
                            },
                            {
                                "type": "recMovie",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/ave_2013-1220-dA30_5-BSC-1_10_rec_keyMov.mp4",
                            },
                            {
                                "type": "tiltMovie",
                                "path": "test/input_files/brt/Assets/RT_TOMO/2013-1220-dA30_5-BSC-1_10/tiltMov_2013-1220-dA30_5-BSC-1_10_ali.mp4",
                            },
                        ],
                    }
                ],
            }
        ]
    }  # noqa
    assert response == expected_response


@pytest.mark.localdata
@pytest.mark.slow
def test_brt_response_partial_failure(mock_nfs_mount, caplog, mock_callback_data):
    from em_workflows.brt.flow import brt_flow

    input_dir = "test/input_files/brt/Projects/RT_TOMO/Partly_Correct/"
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
        x_keep_workdir=False,
        return_state=True,
    )
    assert result.is_completed(), "`result` is not successful!"

    response = {}
    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response
    assert isinstance(response["files"], list)
    assert len(response["files"]) == 2, "There's only 2 files in the input"

    result1, result2 = response["files"]
    assert result1["status"] != result2["status"], "One should have been error"
    expected_response = {
        "files": [
            {
                "primaryFilePath": "test/input_files/brt/Projects/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10.mrc",
                "status": "success",
                "message": None,
                "thumbnailIndex": 0,
                "title": "2013-1220-dA30_5-BSC-1_10",
                "fileMetadata": None,
                "imageSet": [
                    {
                        "imageName": "2013-1220-dA30_5-BSC-1_10",
                        "imageMetadata": None,
                        "assets": [
                            {
                                "type": "thumbnail",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/keyimg_2013-1220-dA30_5-BSC-1_10_ali_ali.060_s.jpg",
                            },
                            {
                                "type": "keyImage",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/2013-1220-dA30_5-BSC-1_10_ali_ali.060.jpg",
                            },
                            {
                                "type": "neuroglancerZarr",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/2013-1220-dA30_5-BSC-1_10.zarr/0",
                                "metadata": {
                                    "shader": "Grayscale",
                                    "dimensions": "XYZ",
                                    "shaderParameters": {
                                        "range": [-785, -419],
                                        "window": [-1242, -278],
                                    },
                                },
                            },
                            {
                                "type": "volume",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/ave_2013-1220-dA30_5-BSC-1_10_rec.mrc",
                            },
                            {
                                "type": "averagedVolume",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/avebin8_ave_2013-1220-dA30_5-BSC-1_10_rec.mrc",
                            },
                            {
                                "type": "recMovie",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/ave_2013-1220-dA30_5-BSC-1_10_rec_keyMov.mp4",
                            },
                            {
                                "type": "tiltMovie",
                                "path": "test/input_files/brt/Assets/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10/tiltMov_2013-1220-dA30_5-BSC-1_10_ali.mp4",
                            },
                        ],
                    }
                ],
            },
            {
                "primaryFilePath": "test/input_files/brt/Projects/RT_TOMO/Partly_Correct/2013-1220-dA30_5-BSC-1_10-Broken.mrc",
                "status": "error",
                "message": "Failure in pipeline step: Batchruntomo conversion",
                "thumbnailIndex": 0,
                "title": "2013-1220-dA30_5-BSC-1_10-Broken",
                "fileMetadata": None,
                "imageSet": [
                    {
                        "imageName": "2013-1220-dA30_5-BSC-1_10-Broken",
                        "imageMetadata": None,
                        "assets": [],
                    }
                ],
            },
        ]
    }  # noqa
    assert response == expected_response
    result_success, result_error = result1, result2
    if result1["status"] == "error":
        result_success, result_error = result2, result1
    assert result_error["status"] == "error"
    assert result_error["message"] is not None
    assert result_success["message"] is None
    assert result_success["imageSet"][0]["assets"] is not None
    assert result_error["imageSet"][0]["assets"] == list()


@pytest.mark.localdata
@pytest.mark.slow
def test_brt_response_all_failure(mock_nfs_mount, caplog, mock_callback_data):
    from em_workflows.brt.flow import brt_flow

    input_dir = "test/input_files/brt/Projects/RT_TOMO/Failure/"
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
        x_keep_workdir=False,
        return_state=True,
    )
    assert result.is_failed()

    response = {}
    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response
    assert isinstance(response["files"], list)
    assert len(response["files"]) == 1, "There's only 1 files in the input"
    result = response["files"][0]
    assert result["status"] == "error"
    assert result["message"], "Error message is empty"
    # Asserting an entire structure of the response to the api server
    expected_response = {
        "files": [
            {
                "primaryFilePath": "test/input_files/brt/Projects/RT_TOMO/Failure/2013-1220-dA30_5-BSC-1_10-Broken.mrc",
                "status": "error",
                "message": "Failure in pipeline step: Batchruntomo conversion",
                "thumbnailIndex": 0,
                "title": "2013-1220-dA30_5-BSC-1_10-Broken",
                "fileMetadata": None,
                "imageSet": [
                    {
                        "imageName": "2013-1220-dA30_5-BSC-1_10-Broken",
                        "imageMetadata": None,
                        "assets": [],
                    }
                ],
            }
        ]
    }  # noqa
    assert response == expected_response
