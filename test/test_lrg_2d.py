def test_input_fname(mock_nfs_mount):
    from em_workflows.lrg_2d_rgb.flow import flow

    # monkeypatch.setattr(Config, "convert_loc", command_loc("convert"))

    state = flow.run(
        input_dir="/test/input_files/lrg_ROI_pngs/Projects",
        no_api=True,
    )
    assert state.is_successful()