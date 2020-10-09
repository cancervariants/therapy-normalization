"""This module provides methods for handling queries"""
from therapy.normalizers import Wikidata, ChEMBL
import re
from uvicorn.config import logger

normalizers = [
    Wikidata(),
    ChEMBL()
]


def normalize(query_str, **params):
    """Return normalized therapy objects given a query_str"""
    resp = {
        'warnings': emit_warnings(query_str),
        'query': query_str,
        'normalizer_matches': dict()
    }
    for normalizer in normalizers:
        results = normalizer.normalize(query_str)
        resp['normalizer_matches'][normalizer.__class__.__name__] = {
            'match_type': results.match_type,
            'records': results.records
        }
    return resp


def emit_warnings(query_str):
    """Emit warnings if query contains non breaking space characters."""
    warnings = dict()
    nbsp = re.search('\xa0|\u00A0|&nbsp;', query_str)
    if nbsp:
        warnings['nbsp'] = 'Query contains non breaking space characters.'
        logger.warning(
            f'Query ({query_str}) contains non breaking space characters.'
        )
    return warnings
