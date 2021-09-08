"""Test that the therapy normalizer works as intended for the ChEMBL source."""
import pytest
from therapy.schemas import Drug, MatchType
from therapy.query import QueryHandler
from tests.conftest import compare_records


@pytest.fixture(scope='module')
def chembl():
    """Build ChEMBL normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search(self, query_str):
            resp = self.query_handler.search_sources(query_str, keyed=True,
                                                     incl='chembl')
            return resp['source_matches']['ChEMBL']
    return QueryGetter()


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'CISPLATIN',
        'concept_id': 'chembl:CHEMBL11359',
        'aliases': [
            'Cisplatin',
            'Cis-Platinum II',
            'Cisplatinum',
            'cis-diamminedichloroplatinum(II)',
            'CIS-DDP',
            'INT-230-6 COMPONENT CISPLATIN',
            'INT230-6 COMPONENT CISPLATIN',
            'NSC-119875',
            'Platinol',
            'Platinol-Aq'
        ],
        'approval_status': 'approved',
        'xrefs': [],
        'associated_with': [],
        'trade_names': [
            'PLATINOL',
            'PLATINOL-AQ'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def l745870():
    """Create a L-745870 drug fixture."""
    params = {
        'label': 'L-745870',
        'concept_id': 'chembl:CHEMBL267014',
        'aliases': [],
        'approval_status': None,
        'xrefs': [],
        'associated_with': [],
        'trade_names': []
    }
    return Drug(**params)


# Test with aliases and trade names > 20
@pytest.fixture(scope='module')
def aspirin():
    """Create an aspirin drug fixture."""
    params = {
        'label': 'ASPIRIN',
        'concept_id': 'chembl:CHEMBL25',
        'aliases': [],
        'approval_status': 'approved',
        'xrefs': [],
        'associated_with': [],
        'trade_names': []
    }
    return Drug(**params)


def test_concept_id_cisplatin(cisplatin, chembl):
    """Test that cisplatin drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    response = chembl.search('chembl:CHEMBL11359')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = chembl.search('CHEMBL11359')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = chembl.search('chembl:chembl11359')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = chembl.search('cHEmbl:chembl11359')
    assert response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = chembl.search('cHEmbl:CHEMBL11359')
    assert response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)


def test_cisplatin_label(cisplatin, chembl):
    """Test that cisplatin drug normalizes to correct drug concept
    as a LABEL match.
    """
    response = chembl.search('CISPLATIN')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 2
    if len(response['records'][0].aliases) >\
            len(response['records'][1].aliases):
        ind = 0
    else:
        ind = 1
    compare_records(response['records'][ind], cisplatin)

    response = chembl.search('cisplatin')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 2
    if len(response['records'][0].aliases) >\
            len(response['records'][1].aliases):
        ind = 0
    else:
        ind = 1
    compare_records(response['records'][ind], cisplatin)


def test_cisplatin_alias(cisplatin, chembl):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    response = chembl.search('cis-diamminedichloroplatinum(II)')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = chembl.search('INT230-6 COMPONENT CISPLATIn')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)


def test_no_match(chembl):
    """Test that a term normalizes to correct drug concept as a NO match."""
    response = chembl.search('cisplati')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0

    # Test white space in between label
    response = chembl.search('L - 745870')
    assert response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    response = chembl.search('')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0


def test_l745870_concept_id(l745870, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    response = chembl.search('chembl:CHEMBL267014')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)

    response = chembl.search('CHEMBL267014')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)

    response = chembl.search('chembl:chembl267014')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)

    response = chembl.search('cHEmbl:chembl267014')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)

    response = chembl.search('cHEmbl:CHEMBL267014')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)


def test_l745870_label(l745870, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as a LABEL match.
    """
    response = chembl.search('L-745870')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)

    response = chembl.search('l-745870')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], l745870)


def test_aspirin_concept_id(aspirin, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    response = chembl.search('chembl:CHEMBL25')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)

    response = chembl.search('CHEMBL25')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)

    response = chembl.search('chembl:chembl25')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)

    response = chembl.search('cHEmbl:chembl25')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)

    response = chembl.search('cHEmbl:CHEMBL25')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)


def test_aspirin_label(aspirin, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as a LABEL match.
    """
    response = chembl.search('ASPIRIN')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)

    response = chembl.search('aspirin')
    assert response['match_type'] ==\
           MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], aspirin)


def test_meta_info(cisplatin, chembl):
    """Test that the meta field is correct."""
    response = chembl.search('cisplatin')
    assert response['source_meta_'].data_license == 'CC BY-SA 3.0'
    assert response['source_meta_'].data_license_url == \
           'https://creativecommons.org/licenses/by-sa/3.0/'
    assert response['source_meta_'].version == '27'
    assert response['source_meta_'].data_url == \
           'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'  # noqa: E501
    assert response['source_meta_'].rdp_url == 'http://reusabledata.org/chembl.html'  # noqa: E501
    assert response['source_meta_'].data_license_attributes == {
        "non_commercial": False,
        "share_alike": True,
        "attribution": True
    }
