"""Test that the chEMBL therapy normalizer works as intended."""
import pytest
from therapy.normalizers import ChEMBL
from therapy.models import Drug
from therapy.normalizers.base import MatchType


@pytest.fixture(scope='module')
def chembl():
    """Create a chEMBL normalizer instance."""
    c = ChEMBL()
    return c


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'CISPLATIN',
        'concept_identifier': 'chembl:CHEMBL11359',
        'aliases': list(),
        'other_identifiers': list(),
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def l745870():
    """Create a L-745870 drug fixture"""
    params = {
        'label': 'L-745870',
        'concept_identifier': 'chembl:CHEMBL267014',
        'aliases': list(('L-745870',)),
        'other_identifiers': list(),
        'max_phase': 0,
    }
    return Drug(**params)


def test_case_sensitive_primary(cisplatin, chembl):
    """Test that cisplatin term normalizes to correct drug concept
    as a PRIMARY match.
    """
    normalizer_response = chembl.normalize('CISPLATIN')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 2
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('chembl:CHEMBL11359')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('CHEMBL11359')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_insensitive_primary(cisplatin, chembl):
    """Tests that cisplatin term normalizes to correct drug concept
    as CASE_INSENSITIVE_PRIMARY, NAMESPACE_CASE_INSENSITIVE matches.
    """
    normalizer_response = chembl.normalize('cisplatin')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response.records) == 2
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('chembl:chembl11359')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('cHEmbl:CHEMBL11359')
    assert normalizer_response.match_type == \
           MatchType.NAMESPACE_CASE_INSENSITIVE
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('cHEmbl:chembl11359')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_sensitive_alias(cisplatin, chembl):
    """Tests that alias term normalizes correctly"""
    normalizer_response = chembl.normalize('cis-diamminedichloroplatinum(II)')
    assert normalizer_response.match_type == MatchType.ALIAS
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_insensitive_alias(cisplatin, chembl):
    """Tests that case-insensitive alias term normalizes correctly"""
    normalizer_response = chembl.normalize('INT230-6 COMPONENT CISPLATIn')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_ALIAS
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_no_match(chembl):
    """Test that term normalizes to NO match"""
    normalizer_response = chembl.normalize('cisplati')
    assert normalizer_response.match_type == MatchType.NO_MATCH
    assert len(normalizer_response.records) == 0


def test_query_with_symbols(l745870, chembl):
    """Test that L-745870 normalizes to PRIMARY and CASE_INSENSITIVE match"""
    normalizer_response = chembl.normalize('L-745870')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_identifier == l745870.concept_identifier
    assert normalized_drug.aliases == l745870.aliases
    assert normalized_drug.concept_identifier == l745870.concept_identifier
    assert normalized_drug.max_phase == l745870.max_phase

    normalizer_response = chembl.normalize('l-745870')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response.records) == 1

    normalizer_response = chembl.normalize('L - 745870')
    assert normalizer_response.match_type == MatchType.NO_MATCH


def test_case_empty_query(chembl):
    """Test that empty query normalizes to NO match"""
    normalizer_response = chembl.normalize('')
    assert normalizer_response.match_type == MatchType.NO_MATCH
    assert len(normalizer_response.records) == 0
