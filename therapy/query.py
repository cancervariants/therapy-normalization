"""This module provides methods for handling queries"""
from therapy.normalizers import Wikidata, ChEMBL

normalizers = [
    Wikidata(),
    ChEMBL()
]


def normalize(query_str, **params):
    """Return normalized therapy objects given a query_str

    Param args:
       * keyed: bool - if true, return response as dict of normalizer
         responses, otherwise, return as a list
    """
    resp = {
        'query': query_str,
    }

    if params['keyed']:
        resp['normalizer_matches'] = dict()
        for normalizer in normalizers:
            results = normalizer.normalize(query_str)
            resp['normalizer_matches'][normalizer.__class__.__name__] = {
                'match_type': results.match_type,
                'records': results.records
            }
    else:
        resp['normalizer_matches'] = list()
        for normalizer in normalizers:
            results = normalizer.normalize(query_str)
            resp['normalizer_matches'].append({
                'normalizer': normalizer.__class__.__name__,
                'match_type': results.match_type,
                'records': results.records,
            })
    return resp
