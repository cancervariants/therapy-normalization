"""Test the therapy querying method."""
from therapy.query import QueryHandler, InvalidParameterException
import pytest


@pytest.fixture(scope='module')
def query_handler():
    """Build query handler test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def normalize(self, query_str, keyed=False, incl='', excl=''):
            resp = self.query_handler.search_sources(query_str=query_str,
                                                     keyed=keyed,
                                                     incl=incl, excl=excl)
            return resp

    return QueryGetter()


@pytest.fixture(scope='module')
def merge_query_handler(mock_database):
    """Provide Merge instance to test cases."""
    class QueryGetter():
        def __init__(self):
            self.query_handler = QueryHandler(db_url='http://localhost:8000')
            self.query_handler.db = mock_database()

        def search_groups(self, query_str):
            return self.query_handler.search_groups(query_str)

    return QueryGetter()


def test_query(query_handler):
    """Test that query returns properly-structured response."""
    resp = query_handler.normalize('cisplatin', keyed=False)
    assert resp['query'] == 'cisplatin'
    matches = resp['source_matches']
    assert isinstance(matches, list)
    assert len(matches) == 6
    wikidata = list(filter(lambda m: m['source'] == 'Wikidata',
                           matches))[0]
    assert len(wikidata['records']) == 1
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'


def test_query_keyed(query_handler):
    """Test that query structures matches as dict when requested."""
    resp = query_handler.normalize('penicillin v', keyed=True)
    matches = resp['source_matches']
    assert isinstance(matches, dict)
    chemidplus = matches['ChemIDplus']
    chemidplus_record = chemidplus['records'][0]
    assert chemidplus_record.label == 'Penicillin V'


def test_query_specify_query_handlers(normalizer):
    """Test inclusion and exclusion of sources in query."""
    # test blank params
    resp = query_handler.normalize('cisplatin', keyed=True)
    matches = resp['source_matches']
    assert len(matches) == 6
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches

    # test partial inclusion
    resp = query_handler.normalize('cisplatin', keyed=True, incl='chembl,ncit')
    matches = resp['source_matches']
    assert len(matches) == 2
    assert 'Wikidata' not in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' not in matches

    # test full inclusion
    sources = 'chembl,ncit,drugbank,wikidata,rxnorm,chemidplus'
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   incl=sources, excl='')
    matches = resp['source_matches']
    assert len(matches) == 6
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test partial exclusion
    resp = query_handler.normalize('cisplatin', keyed=True, excl='chemidplus')
    matches = resp['source_matches']
    assert len(matches) == 5
    assert 'Wikidata' in matches
    assert 'ChemIDplus' not in matches

    # test full exclusion
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   excl='chembl, wikidata, drugbank, ncit, '
                                   'rxnorm, chemidplus')
    matches = resp['source_matches']
    assert len(matches) == 0
    assert 'Wikidata' not in matches
    assert 'ChEMBL' not in matches
    assert 'NCIt' not in matches
    assert 'DrugBank' not in matches

    # test case insensitive
    resp = query_handler.normalize('cisplatin', keyed=True, excl='ChEmBl')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' not in matches
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   incl='wIkIdAtA,cHeMbL')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test error on invalid source names
    with pytest.raises(InvalidParameterException):
        resp = query_handler.normalize('cisplatin', keyed=True, incl='chambl')

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterException):
        resp = query_handler.normalize('cisplatin', keyed=True, incl='chembl',
                                       excl='wikidata')


def test_query_merged(merge_query_handler):
    """Test that the merged concept endpoint handles queries correctly."""
    test_query = "phenobarbital"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['record']['label'] == 'phenobarbital'
