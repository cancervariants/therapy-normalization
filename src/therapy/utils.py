"""Provide useful utilities."""

from collections.abc import Generator

from therapy.database import AbstractDatabase
from therapy.schemas import RecordType, SourceName


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
