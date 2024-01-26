import pytest


@pytest.mark.slow
def test_input_fname(mock_nfs_mount, caplog):
    from em_workflows.lrg_2d_rgb.flow import lrg_2d_flow

    state = lrg_2d_flow(
        file_share="test",
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        no_api=True,
    )
    assert state.is_completed()
    # FIXME error documented in error_test_lrg_2d_input_fname.log
    assert "neuroglancerZarr" in caplog.text, caplog.text


def test_bio2r_environ(mock_nfs_mount, caplog):
    from em_workflows.config import Config
    from em_workflows.file_path import FilePath
    import os
    import tempfile

    assert os.path.exists(Config.bioformats2raw)
    with tempfile.NamedTemporaryFile() as logfile:
        cmd = [Config.bioformats2raw, "--version"]
        FilePath.run(cmd=cmd, log_file=logfile.name)
        assert "Bio-Formats version" in caplog.text
