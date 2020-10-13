"""This module provides methods for handling queries"""
import re
from uvicorn.config import logger

normalizers = []


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
    resp = {
        'warnings': emit_warnings(query_str),
        'query': query_str,
    }

    if not incl and not excl:
        query_normalizers = normalizers[:]
    elif incl and excl:
        detail = "Cannot request both normalizer inclusions and exclusions"
        raise InvalidParameterException(detail)
    elif incl:
        req_normalizers = incl.lower().split(',')
        req_normalizers = [n.strip() for n in req_normalizers]
        query_normalizers = list(filter(
            lambda n: n.__class__.__name__.lower() in req_normalizers,
            normalizers))
        if len(query_normalizers) < len(req_normalizers):
            valid_names = [n.__class__.__name__.lower() for n
                           in query_normalizers]
            invalid_names = set(req_normalizers).difference(valid_names)
            detail = f"Invalid normalizer name(s): {invalid_names}"
            raise InvalidParameterException(detail)
    else:
        req_exclusions = excl.lower().split(',')
        req_exclusions = [n.strip() for n in req_exclusions]
        query_normalizers = list(filter(
            lambda n: n.__class__.__name__.lower() not in req_exclusions,
            normalizers))
        if len(query_normalizers) > len(normalizers) - len(req_exclusions):
            valid_names = [n.__class__.__name__.lower() for n
                           in query_normalizers]
            invalid_names = set(req_exclusions).difference(valid_names)
            detail = f"Invalider normalizer name(s): {invalid_names}"
            raise InvalidParameterException(detail)

    if keyed:
        resp['normalizer_matches'] = dict()
        for normalizer in query_normalizers:
            results = normalizer.normalize(query_str)
            resp['normalizer_matches'][normalizer.__class__.__name__] = {
                'match_type': results.match_type,
                'records': results.records,
                'meta_': results.meta_._asdict(),
            }
    else:
        resp['normalizer_matches'] = list()
        for normalizer in query_normalizers:
            results = normalizer.normalize(query_str)
            resp['normalizer_matches'].append({
                'normalizer': normalizer.__class__.__name__,
                'match_type': results.match_type,
                'records': results.records,
                'meta_': results.meta_._asdict(),
            })
    return resp


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
