"""This module provides methods for handling queries"""
from therapy.normalizers import Wikidata, ChEMBL
from fastapi import HTTPException

normalizers = [
    Wikidata(),
    ChEMBL()
]


def normalize(query_str, keyed='false', incl='', excl='', **params):
    """Fetch normalized therapy objects.

    Args:
        query_str: query, a string, to search for
        keyed: bool - if true, return response as dict keying normalizer names
            to normalizer objects; otherwise, return list of normalizer objects
        incl: str containing comma-separated names of normalizers to use. Will
            exclude all other normalizers. Case-insensitive. Raises
            HTTPException if both incl and excl args are provided, or if
            invalid normalizer names are given.
        excl: str containing comma-separated names of normalizers to exclude.
            Will include all other normalizers. Case-insensitive. Raises
            HTTPException if both incl and excl args are provided, or if
            invalid normalizer names are given.

    Returns:
        Dict containing all matches found in normalizers.
    """
    resp = {
        'query': query_str,
    }

    if not incl and not excl:
        query_normalizers = normalizers[:]
    elif incl and excl:
        detail = "Cannot request both normalizer inclusions and exclusions"
        raise HTTPException(status_code=422, detail=detail)
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
            raise HTTPException(status_code=422, detail=detail)
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
            raise HTTPException(status_code=422, detail=detail)

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
