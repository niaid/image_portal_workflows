# test_brt.py
"""
test_brt.py runs an end-to-end test of the batchruntomo pipeline

NOTE: These tests depend on setup performed in conftest.py
"""
import pytest
import json
import os
import shutil
import tempfile
from pathlib import Path

from em_workflows.brt.flow import copy_template, update_adoc
from em_workflows.config import Config
from em_workflows.utils import utils


def test_update_adoc(mock_nfs_mount):
    """
    Test successful modification of adoc based on a template
    :todo: consider parameterizing this to test many values
    """
    adoc_file = "plastic_brt"
    montage = 0
    gold = 15
    focus = 0
    fiducialless = 1
    trackingMethod = None
    TwoSurfaces = 0
    TargetNumberOfBeads = 20
    LocalAlignments = 0
    THICKNESS = 30

    with tempfile.TemporaryDirectory() as tmp_dir:
        adoc_tmplt = Path(os.path.join(Config.template_dir, f"{adoc_file}.adoc"))
        copied_tmplt = Path(tmp_dir) / f"{adoc_file}.adoc"
        shutil.copy(adoc_tmplt, copied_tmplt)

        env = utils.get_environment()
        mrc_image = "test/input_files/brt_inputs/2013-1220-dA30_5-BSC-1_10.mrc"
        mrc_file = Path(os.path.join(Config.proj_dir(env), mrc_image))

        updated_adoc = update_adoc(
            adoc_fp=copied_tmplt,
            tg_fp=mrc_file,
            montage=montage,
            gold=gold,
            focus=focus,
            fiducialless=fiducialless,
            trackingMethod=trackingMethod,
            TwoSurfaces=TwoSurfaces,
            TargetNumberOfBeads=TargetNumberOfBeads,
            LocalAlignments=LocalAlignments,
            THICKNESS=THICKNESS,
        )

        assert updated_adoc.exists()
        assert copied_tmplt.exists()


def test_update_adoc_bad_surfaces(mock_nfs_mount):
    adoc_file = "plastic_brt"
    montage = 0
    gold = 15
    focus = 0
    fiducialless = 1
    trackingMethod = None
    # NOTE: This value is invalid
    TwoSurfaces = 2
    TargetNumberOfBeads = 20
    LocalAlignments = 0
    THICKNESS = 30

    with tempfile.TemporaryDirectory() as tmp_dir:
        adoc_tmplt = Path(os.path.join(Config.template_dir, f"{adoc_file}.adoc"))
        copied_tmplt = Path(tmp_dir) / f"{adoc_file}.adoc"
        shutil.copy(adoc_tmplt, copied_tmplt)

        env = utils.get_environment()
        mrc_image = "test/input_files/brt_inputs/2013-1220-dA30_5-BSC-1_10.mrc"
        mrc_file = Path(os.path.join(Config.proj_dir(env), mrc_image))

        with pytest.raises(ValueError) as fail_msg:
            update_adoc(
                adoc_fp=copied_tmplt,
                tg_fp=mrc_file,
                montage=montage,
                gold=gold,
                focus=focus,
                fiducialless=fiducialless,
                trackingMethod=trackingMethod,
                TwoSurfaces=TwoSurfaces,
                TargetNumberOfBeads=TargetNumberOfBeads,
                LocalAlignments=LocalAlignments,
                THICKNESS=THICKNESS,
            )
        assert "Unable to resolve SurfacesToAnalyze" in str(fail_msg.value)


def test_copy_template(mock_nfs_mount):
    """
    Tests that adoc template get copied to working directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        copy_template(working_dir=Path(tmp_dir), template_name="plastic_brt")
        copy_template(working_dir=Path(tmp_dir), template_name="cryo_brt")
        tmp_path = Path(tmp_dir)
        assert tmp_path.exists()
        assert Path(tmp_path / "plastic_brt.adoc").exists()
        assert Path(tmp_path / "cryo_brt.adoc").exists()


def test_copy_template_missing(mock_nfs_mount):
    """
    Tests that adoc template get copied to working directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(FileNotFoundError) as fnfe:
            copy_template(working_dir=Path(tmp_dir), template_name="no_such_tmplt")
        assert "no_such_tmplt" in str(fnfe.value)


@pytest.mark.localdata
@pytest.mark.slow
def test_brt(mock_nfs_mount):
    from em_workflows.brt.flow import brt_flow

    input_dir = "test/input_files/brt/Projects/RT_TOMO/"
    if not Path(input_dir).exists():
        pytest.skip(f"Directory {input_dir} doesn't exist")

    # parameters to brt_flow are sensitive, and altering them may result in different response
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

    # parameters to brt_flow are sensitive, and altering them may result in different response
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
        assert result["status"] == "success"
        assert result["message"] is None
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
