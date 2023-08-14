import pytest


@pytest.mark.localdata
@pytest.mark.slow
def test_sem(mock_nfs_mount):
    from em_workflows.sem_tomo.flow import flow

    result = flow.run(
        # FIXME `sem_inputs` directory is missing
        input_dir="/test/input_files/sem_inputs/Projects/",
        tilt_angle="30.2",
        no_api=True,
    )
    assert result.is_successful()
