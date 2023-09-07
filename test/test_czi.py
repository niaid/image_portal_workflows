import shutil
from pathlib import Path
from em_workflows.file_path import FilePath


def test_input_fname(mock_nfs_mount, caplog):
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
    fp_in = Path(
        "/gs1/home/hedwig_dev/image_portal_workflows/image_portal_workflows/test/input_files/IF_czi/Projects/zarr_dir_2/"  # noqa: E501
    )
    fp = FilePath(share_name="test", input_dir=input_dir, fp_in=fp_in)
    d = shutil.copytree(fp_in.as_posix(), f"{fp.working_dir.as_posix()}/{fp_in.name}")
    print(d)
    zars = gen_imageSet(fp)
    print(zars)
