# test_brt.py
"""
test_brt.py runs an end-to-end test of the batchruntomo pipeline

NOTE: These tests depend on setup performed in conftest.py
"""
import pytest
import shutil
import os
import platform
from prefect import task, get_run_logger
from pathlib import Path

from em_workflows.utils import utils
from em_workflows.file_path import FilePath


@pytest.fixture
def hpc_env(monkeypatch):
    def _mock_test_output_dir() -> str:
        """
        Get partial results based on machine type. Alternately could use a HOME env. variable.
        :return: The full path where the previously-created output files are stored
        """
        if platform.system() == "Linux":
            return "/home/macmenaminpe/code/image_portal_workflows/test/input_files/brt_outputs/"
        elif platform.system() == "Darwin":
            return "/Users/mbopf/projects/hedwig/data/brt_outputs/"

    @task
    def _prep_mock_brt_run(
        file_path: FilePath,
        adoc_template: str,
        montage: int,
        gold: int,
        focus: int,
        fiducialless: int,
        trackingMethod: int,
        TwoSurfaces: int,
        TargetNumberOfBeads: int,
        LocalAlignments: int,
        THICKNESS: int,
    ) -> None:
        get_run_logger().info(f"MOCKED {file_path}")
        shutil.copy(
            os.path.join(_mock_test_output_dir(), "2013-1220-dA30_5-BSC-1_10_ali.mrc"),
            file_path.working_dir,
        )
        shutil.copy(
            os.path.join(_mock_test_output_dir(), "2013-1220-dA30_5-BSC-1_10_rec.mrc"),
            file_path.working_dir,
        )

    @task
    def _create_brt_command(adoc_fp: Path) -> str:
        s = f"cp {_mock_test_output_dir()}*.mrc {adoc_fp.parent}"
        get_run_logger().info(f"MOCKED {s}")
        return s

    monkeypatch.setattr(utils, "run_brt", _prep_mock_brt_run)


@pytest.mark.localdata
@pytest.mark.slow
# def test_brt(mock_nfs_mount, hpc_env):
def test_brt(mock_nfs_mount):
    from em_workflows.brt.flow import brt_flow

    result = brt_flow(
        adoc_template="plastic_brt",
        montage=0,
        gold=15,
        focus=0,
        fiducialless=1,
        trackingMethod=0,
        # NOTE: passing these as ints, so can't default to None
        #       should the type be "str"?
        # trackingMethod=None,
        TwoSurfaces=0,
        TargetNumberOfBeads=20,
        LocalAlignments=0,
        THICKNESS=30,
        input_dir="test/input_files/brt_inputs/Projects/",
        no_api=True,
        keep_workdir=True,
        return_state=True,
    )
    assert result.is_completed()
