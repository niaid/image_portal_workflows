#!/usr/bin/env python3
"""
Utilities workflows:
-------------------
These workflows are created simply to aid developers.

- Use #1: Run the tests regularly.
  Test input files can be of big size which can be hard to manage. Since most of the tests are normally executed on
  HPC over local machine, it is deemed okay have a prefect scheduler to run the tests for us.
"""
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
def run_tests() -> str:
    """
    Run pytest and save coverage
    The returned string can be viewed in pytest server artifacts section
    Currently, https://prefect2.hedwig-workflow-api.niaiddev.net/artifacts/key/pytest-cov-report
    """
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
    log_prints=True,
    task_runner=Config.SLURM_EXECUTOR,
)
def pytest_flow() -> None:
    test_report = run_tests.submit()
    publish_artifact.submit(test_report)


if __name__ == "__main__":
    pytest_flow()
