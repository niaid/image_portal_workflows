import json
import os
from pathlib import Path
import pytest


@pytest.mark.slow
@pytest.mark.localdata
@pytest.mark.asyncio
async def test_input_fname(mock_nfs_mount, caplog, mock_reuse_zarr):
    from em_workflows.czi.flow import czi_flow

    state = await czi_flow(
        file_share="test",
        input_dir="test/input_files/IF_czi/Projects/Cropped_Image/",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed()


async def test_no_mount_point_flow_fails(mock_binaries, monkeypatch, caplog):
    """
    If mounted path doesn't exist should fail the flow immediately
    """
    from em_workflows import config
    from em_workflows.czi.flow import czi_flow

    share_name = "INVALID"
    _mock_NFS_MOUNT = {share_name: "/tmp/non-existent-path"}

    monkeypatch.setattr(config, "NFS_MOUNT", _mock_NFS_MOUNT)

    with pytest.raises(RuntimeError):
        await czi_flow(
            file_share=share_name,
            input_dir="test/input_files/IF_czi/Projects/Cropped_Image/",
            x_no_api=True,
        )
    assert f"{share_name} doesn't exist. Failing!" in caplog.text, caplog.text


async def test_czi_workflow_server_response_structure(
    mock_nfs_mount, caplog, mock_reuse_zarr, mock_callback_data
):
    """
    Tests that appropriate structure exists in the callback output
    Tests that there is no duplication in the callback output
    """
    from em_workflows.czi.flow import czi_flow

    input_dir = "test/input_files/IF_czi/Projects/Cropped_Image"
    if not Path(input_dir).exists():
        pytest.skip("Missing input files")

    state = await czi_flow(
        file_share="test",
        input_dir=input_dir,
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed()

    callback_output = {}
    with open(mock_callback_data) as fd:
        callback_output = json.load(fd)

    # Below, assert possible structures and data sanitation checks
    assert "files" in callback_output
    # .czi files and length of the callback.files is equal
    assert len(
        [item for item in os.listdir(input_dir) if item.endswith(".czi")]
    ) == len(callback_output["files"])

    first = callback_output["files"][0]
    # look at the structure of the result
    assert "primaryFilePath" in first
    assert "title" in first
    assert "thumbnailIndex" in first and first["thumbnailIndex"] >= 0
    assert "imageSet" in first and isinstance(first["imageSet"], list)
    assert (
        len(first["imageSet"]) > 1
    ), "One label image and atleast one scene image should exist"
    scene_elements = [
        img for img in first["imageSet"] if img["imageName"] != "label image"
    ]

    # scene structures
    first_scene = scene_elements[0]
    assert "imageName" in first_scene
    assert len(first_scene["assets"]) == 1, "Only one zarr file should have existed"
    assets = first_scene["assets"]
    assert assets[0]["type"] == "neuroglancerZarr"
    nzarr = assets[0]
    assert "path" in nzarr
    assert (
        nzarr["path"] != first["primaryFilePath"]
    ), "The neuroglancer group path should be different from root path"

    label_element = [
        img for img in first["imageSet"] if img["imageName"] == "label image"
    ]
    # label related checks
    assert len(label_element) == 1, "Only one label image was expected"
    label_element = label_element[0]
    assert len(label_element["assets"]) == 1
    assert label_element["assets"][0]["type"] == "thumbnail"
