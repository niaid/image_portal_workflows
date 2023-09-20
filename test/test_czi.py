import json
import os
import shutil
from pathlib import Path
import pytest
from em_workflows.file_path import FilePath


@pytest.fixture
def mock_reuse_zarr(monkeypatch):
    """
    Reuses zarr generated from bioformats2raw
    One of the most expensive operation in tests is using bf2raw command
    This test assumes that we have already ran the test once. If the .zarr
    converted file is found, reuses the file without re-executing bf2raw command
    """
    from em_workflows.utils import neuroglancer as ng

    def _mock_bioformats_gen_zarr(file_path: FilePath):
        zarr_fp = f"{file_path.assets_dir}/{file_path.base}.zarr"
        if Path(zarr_fp).exists():
            print("Reusing existing .zarr files! Avoiding bf2raw command.")
            return
        ng.bioformats_gen_zarr(file_path)

    monkeypatch.setattr(ng, "bioformats_gen_zarr", _mock_bioformats_gen_zarr)


@pytest.mark.slow
@pytest.mark.localdata
def test_input_fname(mock_nfs_mount, caplog, mock_reuse_zarr):
    from em_workflows.czi.flow import flow

    state = flow.run(
        file_share="test",
        input_dir="test/input_files/IF_czi/Projects/smaller",
        no_api=True,
    )
    assert state.is_successful()


def test_rechunk(mock_nfs_mount, caplog):
    from em_workflows.czi.flow import gen_imageSet

    input_dir = Path(
        "/gs1/home/hedwig_dev/image_portal_workflows/image_portal_workflows/test/input_files/IF_czi/Projects/zarr_dir_2/"  # noqa: E501
    )
    if not input_dir.exists():
        pytest.skip("Input dirs need to be put in place properly.")
    fp_in = Path(
        "/gs1/home/hedwig_dev/image_portal_workflows/image_portal_workflows/test/input_files/IF_czi/Projects/zarr_dir_2/"  # noqa: E501
    )
    fp = FilePath(share_name="test", input_dir=input_dir, fp_in=fp_in)
    d = shutil.copytree(fp_in.as_posix(), f"{fp.working_dir.as_posix()}/{fp_in.name}")
    zarrs = gen_imageSet(fp)
    print(d, zarrs)


def test_no_mount_point_flow_fails(mock_binaries, monkeypatch, caplog):
    """
    If mounted path doesn't exist should fail the flow immediately
    """
    from em_workflows import config
    from em_workflows.czi.flow import flow

    share_name = "INVALID"
    _mock_NFS_MOUNT = {share_name: "/tmp/non-existent-path"}

    monkeypatch.setattr(config, "NFS_MOUNT", _mock_NFS_MOUNT)

    state = flow.run(
        file_share=share_name,
        input_dir="test/input_files/IF_czi/Projects/smaller",
        no_api=True,
    )
    assert not state.is_successful()
    assert f"{share_name} doesn't exist. Failing!" in caplog.text, caplog.text


def test_czi_workflow_callback_structure(
    mock_nfs_mount, caplog, mock_reuse_zarr, mock_callback_data
):
    """
    Tests that appropriate structure exists in the callback output
    Tests that there is no duplication in the callback output
    """
    from em_workflows.czi.flow import flow

    input_dir = "test/input_files/IF_czi/Projects/smaller"
    if not Path(input_dir).exists():
        pytest.skip("Missing input files")

    state = flow.run(
        file_share="test",
        input_dir=input_dir,
        no_api=True,
    )
    assert state.is_successful()

    callback_output = {}
    with open(mock_callback_data) as fd:
        callback_output = json.load(fd)

    # Belwo, assert possible structures and data sanitation checks
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
    assert (
        len(first_scene["assets"]) == 2
    ), "One thumbnail and one zarr file should have existed"
    assets = first_scene["assets"]
    assert sorted([assets[0]["type"], assets[1]["type"]]) == [
        "neuroglancerZarr",
        "thumbnail",
    ]
    nzarr = [asset for asset in assets if asset["type"] == "neuroglancerZarr"][0]
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
