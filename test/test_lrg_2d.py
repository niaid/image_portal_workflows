from pathlib import Path
import pytest


@pytest.mark.slow
def test_input_fname(mock_nfs_mount, caplog):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed()
    # FIXME error documented in error_test_lrg_2d_input_fname.log
    assert "neuroglancerZarr" in caplog.text, caplog.text


def test_only_wd_logs_are_copied(mock_nfs_mount, caplog, mock_reuse_zarr):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        x_file_name="even_smaller.png",
        x_no_api=True,
        return_state=True,
    )
    assert state.is_completed()

    asset_path = Path("test/input_files/lrg_ROI_pngs/Assets/even_smaller/")
    print(list(asset_path.iterdir()))
    assert (
        len(list(asset_path.glob("logs*/even_smaller/*"))) > 0
    ), "Log files are missing"
