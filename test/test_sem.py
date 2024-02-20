import json
import pytest
from pathlib import Path


@pytest.mark.localdata
@pytest.mark.slow
def test_sem_server_response(mock_nfs_mount, caplog, mock_callback_data):
    from em_workflows.sem_tomo.flow import sem_tomo_flow

    input_dir = "test/input_files/sem_inputs/Projects/YFV-Asibi"
    if not Path(input_dir).exists():
        pytest.skip("Directory doesn't exist")

    state = sem_tomo_flow(
        file_share="test",
        input_dir=input_dir,
        tilt_angle="30.2",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed(), "Flow run failed"

    response = {}
    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response, "files not in response"
    assert isinstance(response["files"], list), "response.files not a list"
    results = response["files"]
    expected_keys = sorted(
        "primaryFilePath status message thumbnailIndex title fileMetadata imageSet".split()
    )
    expected_imageset_keys = sorted("imageName imageMetadata assets".split())
    expected_asset_types = sorted(
        "thumbnail keyImage neuroglancerZarr averagedVolume recMovie".split()
    )
    for result in results:
        assert expected_keys == sorted(result.keys()), "response keys don't match"
        assert result["status"] == "success"
        assert result["message"] is None
        assert len(result["imageSet"]) == 1, "more than one imageset"
        image_set = result["imageSet"][0]
        assert expected_imageset_keys == sorted(
            list(image_set.keys())
        ), "imageset keys don't match"
        assets = image_set["assets"]
        obtained_asset_types = sorted([asset["type"] for asset in assets])
        assert expected_asset_types == obtained_asset_types, "asset types don't match"
        assert all(["path" in asset for asset in assets]), "path not in all assets"
        assert all(
            [isinstance(asset["path"], str) for asset in assets]
        ), "not all asset.path is str"
        assert all([asset["path"] for asset in assets]), "not all asset.path is valid"


@pytest.mark.localdata
@pytest.mark.slow
def test_sem_partial_failure(mock_nfs_mount, caplog, mock_callback_data):
    from em_workflows.sem_tomo.flow import sem_tomo_flow

    input_dir = "test/input_files/sem_inputs/Projects/Partly_Correct"
    if not Path(input_dir).exists():
        pytest.skip("Directory doesn't exist")

    result = sem_tomo_flow(
        file_share="test",
        input_dir=input_dir,
        tilt_angle="30.2",
        x_no_api=True,
        return_state=True,
    )
    assert result.is_completed()
    ...
    # check for expected result


def test_sem_all_failed(mock_nfs_mount, caplog, mock_callback_data):
    pytest.skip("We do not have failing input files yet")
