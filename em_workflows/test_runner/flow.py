#!/usr/bin/env python3
"""
Utilities workflows:
-------------------
These workflows are created simply to aid developers.

- Use #1: Run the tests regularly.
  Test input files can be of big size which can be hard to manage. Since most of the tests are normally executed on
  HPC over local machine, it is deemed okay have a prefect scheduler to run the tests for us.
- Use #2: They could be part of GH actions and run in AWS
  Data files in HPC are private and are "expected" to stay in NIAID VPN
  So, these tests are to be run in the HPC itself rather than trying to recreate HPC in a cloud env
"""
import os
from pathlib import Path
import subprocess

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

from em_workflows.config import Config
from em_workflows.utils import utils


@task
def publish_artifact(report: str) -> None:
    create_markdown_artifact(
        key="pytest-cov-report",
        markdown=report,
        description="Pytest Coverage Report",
    )


@task
def run_tests(git_branch: str) -> str:
    """
    ***Limit to dev server***
    Run pytest and save coverage
    The returned string can be viewed in pytest server artifacts section
    Currently, https://prefect2.hedwig-workflow-api.niaiddev.net/artifacts/key/pytest-cov-report
    """
    test_path = "test_runner"
    test_dir = Path.home() / test_path
    assert test_dir.exists(), "Make sure test_runner dir exists at HOME"

    os.chdir(test_dir.as_posix())

    # stash the changes, checkout to given branch; pull
    gh_sp = subprocess.run(
        f"git stash; git fetch; git checkout {git_branch}; git pull --rebase;",
        check=False,
        capture_output=True,
        shell=True,
    )
    utils.log(
        f"Git branch set to {git_branch}.\n{gh_sp.returncode=}\n{gh_sp.stdout=}\n{gh_sp.stderr=}"
    )

    pytest_sp = subprocess.run("pytest".split(), check=False, capture_output=True)
    # "test/test_utils.py::test_task_result_persistend_and_accessed_by_hooks"
    utils.log(
        f"Pytest is done. {pytest_sp.returncode=}\n {pytest_sp.stdout=}\n {pytest_sp.stderr=}"
    )
    if pytest_sp.stderr:
        raise RuntimeError(pytest_sp.stderr)
    # coverage.svg is used to show the coverage percentage in the github site
    sp = subprocess.run(
        "coverage-badge -f -o coverage.svg".split(), check=False, capture_output=True
    )
    utils.log(f"Coverage is done. {sp.returncode=}\n {sp.stdout=}\n {sp.stderr=}")
    if sp.stderr:
        raise RuntimeError(sp.stderr)
    report = pytest_sp.stdout.decode()
    # pytest returns a test report with warnings, failure details and more
    # However, the meat of the pytest report is after the ---- coverage: pattern
    # So we are only grabbing the main coverage report to show
    idx = report.find("---------- coverage:")
    return report[idx:]


@flow(
    name="Pytest Runner",
    flow_run_name="Test-on-{git_branch}",
    log_prints=True,
    task_runner=Config.SLURM_EXECUTOR,
)
def pytest_flow(git_branch: str = "dev") -> None:
    test_report = run_tests.submit(git_branch)
    publish_artifact.submit(test_report)


if __name__ == "__main__":
    pytest_flow()
