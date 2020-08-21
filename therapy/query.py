"""This module provides methods for handling queries"""
from therapy.normalizers import Wikidata

normalizers = [
    Wikidata(),
]


def normalize(query_str, **params):
    """Return normalized therapy objects given a query_str"""
    resp = dict()
    for normalizer in normalizers:
        results = normalizer.normalize(query_str)
        resp[normalizer.__class__.__name__] = results
    return resp
