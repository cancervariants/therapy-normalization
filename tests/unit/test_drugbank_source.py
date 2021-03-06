"""Test that the therapy normalizer works as intended for the DrugBank
source.
"""
import pytest

from tests.conftest import compare_response
from therapy.query import QueryHandler
from therapy.schemas import Drug, MatchType
import re


@pytest.fixture(scope='module')
def drugbank():
    """Build DrugBank normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search(self, query_str):
            resp = self.query_handler.search_sources(query_str, keyed=True,
                                                     incl='drugbank')
            return resp['source_matches']['DrugBank']

        def fetch_meta(self):
            return self.query_handler._fetch_meta('DrugBank')

    return QueryGetter()


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'Cisplatin',
        'concept_id': 'drugbank:DB00515',
        'aliases': [
            'CDDP',
            'Cis-DDP',
            'cis-diamminedichloroplatinum(II)',
            'Cisplatin',
            'APRD00359',
            'cisplatino',
        ],
        'xrefs': [
            'chemidplus:15663-27-1',
        ],
        'trade_names': [],
        'associated_with': [
            "unii:Q20Q21Q62J",
            "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L"
        ],
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def bentiromide():
    """Create a bentiromide drug fixture."""
    params = {
        'label': 'Bentiromide',
        'concept_id': 'drugbank:DB00522',
        'aliases': [
            'APRD00818',
            'Bentiromide',
            'Bentiromido',
            'Bentiromidum',
            'BTPABA',
            'PFT',
        ],
        'xrefs': [
            'chemidplus:37106-97-1',
        ],
        'trade_names': [],
        'associated_with': [
            "unii:239IF5W61J",
            "inchikey:SPPTWHFVYKCNNK-FQEVSTJZSA-N"
        ],
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def aloe_ferox_leaf():
    """Create aloe ferox leaf fixture. Record has >20 aliases, so ETL deletes
    all of them.
    """
    params = {
        'concept_id': 'drugbank:DB14257',
        'label': 'Aloe ferox leaf',
        'aliases': [],
        'xrefs': [],
        'trade_names': [],
        "associated_with": ["unii:0D145J8EME"]
    }
    return Drug(**params)


def test_concept_id_match(drugbank, cisplatin, bentiromide, aloe_ferox_leaf):
    """Test that concept ID query resolves to correct record."""
    response = drugbank.search('drugbank:DB00515')
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search('DB00515')
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search('drugbank:db00515')
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search('Drugbank:db00515')
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search('druGBank:DB00515')
    compare_response(response, MatchType.CONCEPT_ID, cisplatin)

    response = drugbank.search('drugbank:DB00522')
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search('DB00522')
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search('drugbank:db00522')
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search('Drugbank:db00522')
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search('druGBank:DB00522')
    compare_response(response, MatchType.CONCEPT_ID, bentiromide)

    response = drugbank.search('drugbank:DB14257')
    compare_response(response, MatchType.CONCEPT_ID, aloe_ferox_leaf)


def test_label_match(drugbank, cisplatin, bentiromide, aloe_ferox_leaf):
    """Test that label query resolves to correct record."""
    response = drugbank.search('cisplatin')
    compare_response(response, MatchType.LABEL, cisplatin)

    response = drugbank.search('cisplatin')
    compare_response(response, MatchType.LABEL, cisplatin)

    response = drugbank.search('Bentiromide')
    compare_response(response, MatchType.LABEL, bentiromide)

    response = drugbank.search('bentiromide')
    compare_response(response, MatchType.LABEL, bentiromide)

    response = drugbank.search('aloe ferox leaf')
    compare_response(response, MatchType.LABEL, aloe_ferox_leaf)


def test_alias_match(drugbank, cisplatin, bentiromide, aloe_ferox_leaf):
    """Test that alias query resolves to correct record."""
    response = drugbank.search('CISPLATINO')
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = drugbank.search('Cis-ddp')
    compare_response(response, MatchType.ALIAS, cisplatin)

    response = drugbank.search('APRD00818')
    compare_response(response, MatchType.ALIAS, bentiromide)

    response = drugbank.search('PFT')
    compare_response(response, MatchType.ALIAS, bentiromide)

    # verify aliases > 20 not stored
    response = drugbank.search('Aloe Capensis')
    assert response['match_type'] == MatchType.NO_MATCH

    response = drugbank.search('Aloe Ferox Juice')
    assert response['match_type'] == MatchType.NO_MATCH


def test_xref_match(drugbank, cisplatin, bentiromide):
    """Test that xref query resolves to correct record."""
    response = drugbank.search('chemidplus:15663-27-1')
    compare_response(response, MatchType.XREF, cisplatin)

    response = drugbank.search('chemidplus:37106-97-1')
    compare_response(response, MatchType.XREF, bentiromide)


def test_assoc_with_match(drugbank, cisplatin, bentiromide, aloe_ferox_leaf):
    """Test that associated_with query resolves to correct record."""
    response = drugbank.search('inchikey:lxzzyrpgzafole-uhfffaoysa-l')
    compare_response(response, MatchType.ASSOCIATED_WITH, cisplatin)

    response = drugbank.search('unii:239if5w61j')
    compare_response(response, MatchType.ASSOCIATED_WITH, bentiromide)

    response = drugbank.search('UNII:0D145J8EME')
    compare_response(response, MatchType.ASSOCIATED_WITH, aloe_ferox_leaf)


def test_no_match(drugbank):
    """Test that a term normalizes to correct drug concept as a NO match."""
    response = drugbank.search('lepirudi')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0

    # Test white space in between id
    response = drugbank.search('DB 00001')
    assert response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    response = drugbank.search('')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0


def test_meta_info(drugbank):
    """Test that the meta field is correct."""
    response = drugbank.fetch_meta()
    assert response.data_license == 'CC0 1.0'
    assert response.data_license_url == 'https://creativecommons.org/publicdomain/zero/1.0/'  # noqa: E501
    assert re.match(r'[0-9]+\.[0-9]+\.[0-9]', response.version)
    assert response.data_url == 'https://go.drugbank.com/releases/latest#open-data'  # noqa: E501
    assert response.rdp_url == 'http://reusabledata.org/drugbank.html'  # noqa: E501
    assert response.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False
    }
