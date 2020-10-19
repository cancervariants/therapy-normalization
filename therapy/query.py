"""This module provides methods for handling queries"""
import re
from typing import List
from uvicorn.config import logger
from therapy.etl import ChEMBL, Wikidata  # noqa F401
from therapy.database import Base, engine, SessionLocal
from therapy.models import Therapy, Alias, OtherIdentifier, TradeName, Meta  # noqa F401


class InvalidParameterException(Exception):
    """Exception for invalid parameter args provided by the user"""

    def __init__(self, message):
        """Create new instance

        Args:
            message: string describing the nature of the error
        """
        super().__init__(message)


def normalize(query_str, keyed='false', incl='', excl='', **params):
    """Fetch normalized therapy objects.

    Args:
        query_str: query, a string, to search for
        keyed: bool - if true, return response as dict keying normalizer names
            to normalizer objects; otherwise, return list of normalizer objects
        incl: str containing comma-separated names of normalizers to use. Will
            exclude all other normalizers. Case-insensitive. Raises
            InvalidParameterException if both incl and excl args are provided,
            or if invalid normalizer names are given.
        excl: str containing comma-separated names of normalizers to exclude.
            Will include all other normalizers. Case-insensitive. Raises
            InvalidParameterException if both incl and excl args are provided,
            or if invalid normalizer names are given.

    Returns:
        Dict containing all matches found in normalizers.
    """
    sources = ['wikidata', 'chembl']

    resp = {
        'warnings': emit_warnings(query_str),
        'query': query_str,
    }

    if not incl and not excl:
        query_sources = sources
    elif incl and excl:
        detail = "Cannot request both normalizer inclusions and exclusions"
        raise InvalidParameterException(detail)
    elif incl:
        req_sources = incl.lower().split(',')
        req_sources = [n.strip() for n in req_sources]
        invalid_sources = []
        for source in req_sources:
            if source not in sources:
                invalid_sources.append(source)
        if invalid_sources:
            detail = f"Invalid normalizer name(s): {invalid_sources}"
            raise InvalidParameterException(detail)
        query_sources = req_sources
    else:
        req_exclusions = incl.lower().split(',')
        req_exclusions = [n.strip() for n in req_exclusions]
        query_sources = list(filter(
            lambda s: s not in req_exclusions,
            sources
        ))
        if len(query_sources) > len(sources) - len(req_exclusions):
            invalid_names = set(req_exclusions).difference(sources)
            detail = f"Invalider normalizer name(s): {invalid_names}"
            raise InvalidParameterException(detail)

    if keyed:
        return response_keyed()
    else:
        return response_list()

    # if keyed:
    #     resp['normalizer_matches'] = dict()
    #     for normalizer in query_normalizers:
    #         results = normalizer.normalize(query_str)
    #         resp['normalizer_matches'][normalizer.__class__.__name__] = {
    #             'match_type': results.match_type,
    #             'records': results.records,
    #             'meta_': results.meta_._asdict(),
    #         }
    # else:
    #     resp['normalizer_matches'] = list()
    #     for normalizer in query_normalizers:
    #         results = normalizer.normalize(query_str)
    #         resp['normalizer_matches'].append({
    #             'normalizer': normalizer.__class__.__name__,
    #             'match_type': results.match_type,
    #             'records': results.records,
    #             'meta_': results.meta_._asdict(),
    #         })
    # return resp
    return resp


def response_keyed(query: str, sources: List[str]):
    """Return response as dict where key is source name and value
    is a list of records
    """
    query = query.strip()
    if query == '':
        # return response w/ MatchType.NO_MATCH
        pass

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()  # noqa F841

    # GROUP 1
    # first check if therapies.concept_id = query
    # check if therapies.concept_id ILIKE query
    # check if therapies.label = query
    # check if therapies.label ILIKE query
    # get aliases, other_IDs, trade_name based on concept id

    # GROUP 2
    # check if aliases.alias = query
    # check if aliases.alias ILIKE query
    # check if trade_names.trade_name = query TODO special match type?
    # check if trade_names.trade_name ILIKE query
    # get concept ID from ^ that table, get row from therapies, get other rows
    # based on concept ID

    # return NO MATCH
    pass


def response_list():
    """Return response as list, where the first key-value in each item
    is the source name
    """
    pass


def emit_warnings(query_str):
    """Emit warnings if query contains non breaking space characters."""
    warnings = None
    nbsp = re.search('\xa0|\u00A0|&nbsp;', query_str)
    if nbsp:
        warnings = {'nbsp': 'Query contains non breaking space characters.'}
        logger.warning(
            f'Query ({query_str}) contains non breaking space characters.'
        )
    return warnings
