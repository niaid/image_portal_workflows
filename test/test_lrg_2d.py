import json
from pathlib import Path
import pytest


@pytest.mark.slow
def test_lrg_2d_flow_server_response(mock_nfs_mount, mock_callback_data):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed()

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
    expected_asset_types = sorted("thumbnail keyImage neuroglancerZarr".split())
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


def test_lrg_2d_flow_failure_server_response(
    monkeypatch, mock_nfs_mount, mock_callback_data
):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow
    from em_workflows.lrg_2d_rgb.flow import ng

    original_gen_zarr = ng.bioformats_gen_zarr

    def fake_gen_zarr(file_path, input_fname):
        print(f"Fake called for {input_fname=}")
        if "even_smaller" in input_fname:
            raise RuntimeError(f"Bad input file {input_fname}")
        return original_gen_zarr(file_path, input_fname)

    monkeypatch.setattr(ng, "bioformats_gen_zarr", fake_gen_zarr)

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        x_file_name="even_smaller.png",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed(), "Flow failed"

    response = {}
    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response, "files not in response"
    assert isinstance(response["files"], list), "response.files not a list"
    results = response["files"]
    result = results[0]
    assert result["status"] == "error"
    assert "gen_zarr" in result["message"]


@pytest.mark.parametrize("fails_for", ["even_smaller_broken"])
def test_lrg_2d_flow_partial_failure_server_response(
    monkeypatch, mock_nfs_mount, mock_callback_data, fails_for
):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow
    from em_workflows.lrg_2d_rgb.flow import ng

    original_gen_zarr = ng.bioformats_gen_zarr

    def fake_gen_zarr(file_path, input_fname):
        if fails_for in input_fname:
            raise RuntimeError(f"Bad input file {input_fname}")
        return original_gen_zarr(file_path, input_fname)

    monkeypatch.setattr(ng, "bioformats_gen_zarr", fake_gen_zarr)

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects/",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed(), "Flow failed"

    response = {}
    with open(mock_callback_data) as fd:
        response = json.load(fd)

    assert "files" in response, "files not in response"
    assert isinstance(response["files"], list), "response.files not a list"
    results = response["files"]
    for result in results:
        if fails_for in result["primaryFilePath"]:
            assert result["status"] == "error"
            assert "gen_zarr" in result["message"]
        else:
            assert result["status"] == "success"
            assert result["message"] is None


def test_only_wd_logs_are_copied(mock_nfs_mount):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        x_file_name="even_smaller.png",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed(), "lrg flow run failed"

    asset_path = Path("test/input_files/lrg_ROI_pngs/Assets/even_smaller/")
    print(list(asset_path.iterdir()))
    assert (
        len(list(asset_path.glob("logs*/even_smaller/*"))) > 0
    ), "Log files are missing"
