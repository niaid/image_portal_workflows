#!/usr/bin/env python3
"""
Utilities workflows:
-------------------
These workflows are created simply to aid developers.

- Use #1: Run the tests regularly.
  Test input files can be of big size which can be hard to manage. Since most of the tests are normally executed on
  HPC over local machine, it is deemed okay have a prefect scheduler to run the tests for us.
"""
from pathlib import Path
import subprocess
from time import time

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

from em_workflows.config import Config
from em_workflows.utils import utils

RECENT_FILE = "coverage.svg"
RECENT_HOUR = 1.0


@task
def publish_artifact(report: str):
    return create_markdown_artifact(
        key="pytest-cov-report",
        markdown=report,
        description="Pytest Coverage Report",
    )


@task
def is_run_recently():
    """
    Checks if the test has been run recently using locally saved file
    """
    path = Path(RECENT_FILE)
    if not path.exists():
        return False
    last_modified = path.stat().st_mtime
    is_recent = ((time() - last_modified) / 60) < RECENT_HOUR  # is run recently
    utils.log(f"Pytest was run recently: {is_recent}")
    return is_recent


@task
def run_tests() -> str:
    """
    Run pytest and save coverage
    """
    pytest_sp = subprocess.run("pytest".split(), check=False, capture_output=True)
    # "test/test_utils.py::test_task_result_persistend_and_accessed_by_hooks"
    utils.log(
        f"Pytest is done. {pytest_sp.returncode=}\n {pytest_sp.stdout=}\n {pytest_sp.stderr=}"
    )
    if pytest_sp.stderr:
        raise RuntimeError(pytest_sp.stderr)
    sp = subprocess.run(
        f"coverage-badge -f -o {RECENT_FILE}".split(), check=False, capture_output=True
    )
    utils.log(f"Coverage is done. {sp.returncode=}\n {sp.stdout=}\n {sp.stderr=}")
    if sp.stderr:
        raise RuntimeError(sp.stderr)
    report = pytest_sp.stdout.decode()
    idx = report.find("---------- coverage:")
    return report[idx:]


@flow(
    name="Pytest Runner",
    log_prints=True,
    task_runner=Config.SLURM_EXECUTOR,
)
def pytest_flow(force_run: bool = False):
    if not force_run:
        is_run_recent = is_run_recently.submit().result()
        if is_run_recent:
            return
    test_report = run_tests.submit()
    return publish_artifact.submit(test_report)


if __name__ == "__main__":
    pytest_flow(True)
