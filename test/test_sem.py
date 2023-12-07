import pytest
from pathlib import Path


@pytest.mark.localdata
@pytest.mark.slow
def test_sem(mock_nfs_mount):
    from em_workflows.sem_tomo.flow import sem_tomo_flow

    input_dir = "test/input_files/sem_inputs/Projects/"
    if not Path(input_dir).exists():
        pytest.skip("Directory doesn't exist")

    result = sem_tomo_flow(
        file_share="test",
        input_dir=input_dir,
        tilt_angle="30.2",
        x_no_api=True,
        return_state=True,
    )
    assert result.is_completed()
