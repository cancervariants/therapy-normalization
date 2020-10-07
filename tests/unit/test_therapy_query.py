"""Test therapy querying method"""
from therapy.query import normalize, InvalidParameterException
import pytest


def test_query():
    """Test that query returns properly-structured response"""
    resp = normalize('cisplatin', keyed=False)
    assert resp['query'] == 'cisplatin'
    matches = resp['normalizer_matches']
    assert isinstance(matches, list)
    assert len(matches) == 2
    wikidata = list(filter(lambda m: m['normalizer'] == 'Wikidata',
                           matches))[0]
    assert len(wikidata['records']) == 1
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'


def test_query_keyed():
    """Test that query structures matches as dict when requested"""
    resp = normalize('cisplatin', keyed=True)
    matches = resp['normalizer_matches']
    assert isinstance(matches, dict)
    wikidata = matches['Wikidata']
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'


def test_query_specify_normalizers():
    """Test inclusion and exclusion of normalizers in query"""
    # test blank params
    resp = normalize('cisplatin', keyed=True)
    matches = resp['normalizer_matches']
    assert len(matches) == 2
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test partial inclusion
    resp = normalize('cisplatin', keyed=True, incl='chembl')
    matches = resp['normalizer_matches']
    assert len(matches) == 1
    assert 'Wikidata' not in matches
    assert 'ChEMBL' in matches

    # test full inclusion
    resp = normalize('cisplatin', keyed=True, incl='chembl,wikidata',
                     excl='')
    matches = resp['normalizer_matches']
    assert len(matches) == 2
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test partial exclusion
    resp = normalize('cisplatin', keyed=True, excl='chembl')
    matches = resp['normalizer_matches']
    assert len(matches) == 1
    assert 'Wikidata' in matches
    assert 'ChEMBL' not in matches

    # test full exclusion
    resp = normalize('cisplatin', keyed=True, excl='chembl,wikidata')
    matches = resp['normalizer_matches']
    assert len(matches) == 0
    assert 'Wikidata' not in matches
    assert 'ChEMBL' not in matches

    # test case insensitive
    resp = normalize('cisplatin', keyed=True, excl='ChEmBl')
    matches = resp['normalizer_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' not in matches
    resp = normalize('cisplatin', keyed=True, incl='wIkIdAtA,cHeMbL')
    matches = resp['normalizer_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test error on invalid normalizer names
    with pytest.raises(InvalidParameterException):
        resp = normalize('cisplatin', keyed=True, incl='chambl')

    # assert resp is error # TODO ??
    with pytest.raises(InvalidParameterException):
        resp = normalize('cisplatin', keyed=True, excl='wakidata')

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterException):
        resp = normalize('cisplatin', keyed=True, incl='chembl',
                         excl='wikidata')
