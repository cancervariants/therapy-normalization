"""Test NCIT source"""
import pytest
from therapy.schemas import Drug, MatchType
from therapy.query import QueryHandler
from tests.conftest import compare_records


@pytest.fixture(scope='module')
def ncit():
    """Build NCIt normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search(self, query_str):
            resp = self.query_handler.search_sources(query_str, keyed=True,
                                                     incl='ncit')
            return resp['source_matches']['NCIt']
    return QueryGetter()


@pytest.fixture(scope='module')
def voglibose():
    """Create a voglibose drug fixture.."""
    params = {
        'label': 'Voglibose',
        'concept_id': 'ncit:C95221',
        'aliases': ['3,4-Dideoxy-4-((2-Hydroxy-1-(Hydroxymethyl)Ethyl)Amino)-2-C-(Hydroxymethyl)-D-Epi-Inositol',  # noqa F401
                    'A-71100', 'AO-128', 'Basen',
                    'N-(1,3-Dihydroxy-2-Propyl)Valiolamine', 'VOGLIBOSE'],
        'other_identifiers': ['chemidplus:83480-29-9'],
        'xrefs': ['fda:S77P977AG8', 'umls:C0532578'],
        'approval_status': None,
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def apricoxib():
    """Create an apricoxib drug fixture."""
    params = {
        'label': 'Apricoxib',
        'concept_id': 'ncit:C74021',
        'aliases': ['APRICOXIB', 'COX-2 Inhibitor TG01', 'CS-706', 'R-109339',
                    'TG01', 'TP2001'],
        'other_identifiers': ['chemidplus:197904-84-0'],
        'xrefs': ['fda:5X5HB3VZ3Z', 'umls:C1737955'],
        'approval_status': None,
        'trade_names': []
    }
    return Drug(**params)


# Test aliases > 20
@pytest.fixture(scope='module')
def trastuzumab():
    """Create a Trastuzumab drug fixture."""
    params = {
        'label': 'Trastuzumab',
        'concept_id': 'ncit:C1647',
        'aliases': [],
        'other_identifiers': ['chemidplus:180288-69-1'],
        'xrefs': ['umls:C0728747', 'fda:P188ANX8CK'],
        'approval_status': None,
        'trade_names': []
    }
    return Drug(**params)


def test_voglibose(voglibose, ncit):
    """Test that Voglibose successfully matches."""
    response = ncit.search('ncit:C95221')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('NCIT:C95221')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('NCIt:c95221')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('C95221')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('voglibose')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('voglibose')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('BASEN')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('AO-128')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('chemidplus:83480-29-9')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)

    response = ncit.search('fda:S77P977AG8')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], voglibose)


def test_apricoxib(apricoxib, ncit):
    """Test that apricoxib drug normalizes to correct drug concept."""
    response = ncit.search('ncit:C74021')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('NCIt:C74021')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('ncit:c74021')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('C74021')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('C74021')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('Apricoxib')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('APRICOXIB')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('CHEMIDPLUS:197904-84-0')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)

    response = ncit.search('fda:5X5HB3VZ3Z')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], apricoxib)


def test_trastuzumab(trastuzumab, ncit):
    """Test that trastuzumab successfully matches."""
    response = ncit.search('ncit:C1647')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('NCIT:C1647')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('NCIt:c1647')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('C1647')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('trastuzumab')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('Trastuzumab')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('chemidplus:180288-69-1')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)

    response = ncit.search('fda:P188ANX8CK')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], trastuzumab)


def test_case_no_match(ncit):
    """Test that a term normalizes to NO match."""
    response = ncit.search('voglibo')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0

    # Test white space in between label
    response = ncit.search('Volgibo')
    assert response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    response = ncit.search('')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0


def test_meta_info(voglibose, ncit):
    """Test that the meta field is correct."""
    response = ncit.search('voglibose')
    assert response['source_meta_'].data_license == 'CC BY 4.0'
    assert response['source_meta_'].data_license_url == \
        'https://creativecommons.org/licenses/by/4.0/legalcode'
    assert response['source_meta_'].version == '20.09d'
    assert response['source_meta_'].data_url == \
        "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/2020/20.09d_Release/"  # noqa: E501
    assert response['source_meta_'].rdp_url == 'http://reusabledata.org/ncit.html'  # noqa: E501
    assert response['source_meta_'].data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True
    }
