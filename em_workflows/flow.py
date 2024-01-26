from typing import List

from prefect import allow_failure

from em_workflows.file_path import FilePath
from em_workflows.utils import utils


def callback_with_cleanup(
    fps: List[FilePath],
    callback_result: List,
    no_api: bool = False,
    callback_url: str = None,
    token: str = None,
    keep_workdir: bool = False,
):
    cp_wd_logs_to_assets = utils.copy_workdir_logs.map(fps, wait_for=[callback_result])
    filtered_callback = utils.filter_results(callback_result)

    cb = utils.send_callback_body(
        no_api=no_api,
        token=token,
        callback_url=callback_url,
        files_elts=filtered_callback,
    )
    utils.cleanup_workdir(
        fps,
        keep_workdir,
        wait_for=[allow_failure(cb), allow_failure(cp_wd_logs_to_assets)],
    )
