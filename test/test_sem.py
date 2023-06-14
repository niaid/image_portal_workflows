import pytest


@pytest.mark.localdata
@pytest.mark.slow
def test_sem(mock_nfs_mount):
    from em_workflows.sem_tomo.flow import sem_flow

    state = sem_flow(
        input_dir="/test/input_files/sem_inputs/Projects/",
        tilt_angle=30.2,
        no_api=True,
        return_state=True,
    )
    assert state.is_completed()
