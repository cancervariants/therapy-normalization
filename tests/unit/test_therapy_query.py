"""Test the therapy querying method."""
from therapy.query import Normalizer, InvalidParameterException
import pytest


@pytest.fixture(scope='module')
def normalizer():
    """Build normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = Normalizer()

        def normalize(self, query_str, keyed=False, incl='', excl=''):
            resp = self.normalizer.normalize(query_str=query_str, keyed=keyed,
                                             incl=incl, excl=excl)
            return resp

    return QueryGetter()


def test_query(normalizer):
    """Test that query returns properly-structured response."""
    resp = normalizer.normalize('cisplatin', keyed=False)
    assert resp['query'] == 'cisplatin'
    matches = resp['source_matches']
    assert isinstance(matches, list)
    assert len(matches) == 5
    wikidata = list(filter(lambda m: m['source'] == 'Wikidata',
                           matches))[0]
    assert len(wikidata['records']) == 1
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'


def test_query_keyed(normalizer):
    """Test that query structures matches as dict when requested."""
    resp = normalizer.normalize('cisplatin', keyed=True)
    matches = resp['source_matches']
    assert isinstance(matches, dict)
    chemidplus = matches['ChemIDplus']
    chemidplus_record = chemidplus['records'][0]
    assert chemidplus_record.label == 'cisplatin'


def test_query_specify_normalizers(normalizer):
    """Test inclusion and exclusion of normalizers in query."""
    # test blank params
    resp = normalizer.normalize('cisplatin', keyed=True)
    matches = resp['source_matches']
    assert len(matches) == 5
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches

    # test partial inclusion
    resp = normalizer.normalize('cisplatin', keyed=True, incl='chembl,ncit')
    matches = resp['source_matches']
    assert len(matches) == 2
    assert 'Wikidata' not in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' not in matches

    # test full inclusion
    resp = normalizer.normalize('cisplatin', keyed=True,
                                incl='chembl,wikidata', excl='')
    matches = resp['source_matches']
    assert len(matches) == 2
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test partial exclusion
    resp = normalizer.normalize('cisplatin', keyed=True, excl='chemidplus')
    matches = resp['source_matches']
    assert len(matches) == 4
    assert 'Wikidata' in matches
    assert 'ChemIDplus' not in matches

    # test full exclusion
    resp = normalizer.normalize('cisplatin', keyed=True, excl='chembl, wikidata, drugbank, ncit, chemidplus')  # noqa: E501
    matches = resp['source_matches']
    assert len(matches) == 0
    assert 'Wikidata' not in matches
    assert 'ChEMBL' not in matches
    assert 'NCIt' not in matches
    assert 'DrugBank' not in matches

    # test case insensitive
    resp = normalizer.normalize('cisplatin', keyed=True, excl='ChEmBl')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' not in matches
    resp = normalizer.normalize('cisplatin', keyed=True,
                                incl='wIkIdAtA,cHeMbL')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test error on invalid normalizer names
    with pytest.raises(InvalidParameterException):
        resp = normalizer.normalize('cisplatin', keyed=True, incl='chambl')

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterException):
        resp = normalizer.normalize('cisplatin', keyed=True, incl='chembl',
                                    excl='wikidata')
