"""Provide useful utilities."""

import logging
from collections.abc import Generator
from logging.handlers import RotatingFileHandler

from therapy.database import AbstractDatabase
from therapy.schemas import RecordType, SourceName


def initialize_logs(log_level: int = logging.INFO) -> None:
    """Configure logging.

    :param log_level: app log level to set
    """
    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(log_level)
    formatter = logging.Formatter(
        "[%(asctime)s] - %(name)s - %(levelname)s : %(message)s"
    )
    fh = RotatingFileHandler(f"{__package__}.log", maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(formatter)
    root.addHandler(fh)


def get_term_mappings(
    database: AbstractDatabase, scope: RecordType | SourceName
) -> Generator[dict, None, None]:
    """Produce dict objects for known concepts (name + ID) plus other possible relevant terms

    Use in downstream applications such as autocompletion.

    :param database: instance of DB connection to get records from
    :param scope: constrain record scope, either to a kind of record or to a specific source
    :return: Generator yielding mapping objects
    """
    if isinstance(scope, SourceName):
        record_type = RecordType.IDENTITY
        src_name = scope
    elif isinstance(scope, RecordType):
        record_type = scope
        src_name = None
    else:
        raise TypeError

    for record in database.get_all_records(record_type=record_type):
        if src_name and record["src_name"] != src_name:
            continue
        yield {
            "concept_id": record["concept_id"],
            "label": record.get("label"),
            "aliases": record.get("aliases", []),
            "trade_names": record.get("trade_names", []),
            "xrefs": record.get("xrefs", []) + record.get("associated_with", []),
        }
