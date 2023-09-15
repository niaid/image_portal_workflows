import pytest
import shutil
from pathlib import Path
from em_workflows.file_path import FilePath


@pytest.mark.slow
@pytest.mark.localdata
def test_input_fname(mock_nfs_mount, caplog):
    from em_workflows.czi.flow import flow

    state = flow.run(
        file_share="test",
        input_dir="test/input_files/IF_czi/Projects/smaller",
        no_api=True,
    )
    assert state.is_successful()


@pytest.mark.skip(reason="input dirs need to be put in place properly.")
def test_rechunk(mock_nfs_mount, caplog):
    from em_workflows.czi.flow import gen_imageSet

    input_dir = Path(
        "/gs1/home/hedwig_dev/image_portal_workflows/image_portal_workflows/test/input_files/IF_czi/Projects/zarr_dir_2/"  # noqa: E501
    )
    fp_in = Path(
        "/gs1/home/hedwig_dev/image_portal_workflows/image_portal_workflows/test/input_files/IF_czi/Projects/zarr_dir_2/"  # noqa: E501
    )
    fp = FilePath(share_name="test", input_dir=input_dir, fp_in=fp_in)
    d = shutil.copytree(fp_in.as_posix(), f"{fp.working_dir.as_posix()}/{fp_in.name}")
    print(d)
    zars = gen_imageSet(fp)
    print(zars)


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
