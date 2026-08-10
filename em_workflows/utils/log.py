from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def log(msg: str) -> None:
    # falls back to print when called outside a Prefect flow context
    try:
        get_run_logger().info(msg)
    except MissingContextError:
        print(msg)
