"""Provide functions and utilities related to application logging."""

import logging


def _quiet_upstream_libs() -> None:
    """Turn off debug logging for chatty upstream library loggers."""
    for lib in (
        "boto3",
        "botocore",
        "urllib3",
    ):
        logging.getLogger(lib).setLevel(logging.INFO)


def configure_logs(
    log_file: str | None = None,
    log_level: int = logging.DEBUG,
    quiet_upstream: bool = True,
) -> None:
    """Configure logging.

    :param log_filename: location to put log file at
    :param log_level: global log level to set
    :param quiet_upstream: if True, turn off debug logging for a selection of libraries
    """
    if log_file is None:
        log_file = "therapy.log"
    if quiet_upstream:
        _quiet_upstream_libs()
    logging.basicConfig(
        filename=log_file,
        format="[%(asctime)s] - %(name)s - %(levelname)s : %(message)s",
    )
    logger = logging.getLogger(__package__)
    logger.setLevel(log_level)
