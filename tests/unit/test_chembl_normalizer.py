"""Test that the therapy normalizer works as intended."""
import pytest
from therapy.normalizers import ChEMBL
from therapy.models import Drug
from therapy.normalizers.base import MatchType


@pytest.fixture(scope='module')
def chembl():
    """Create a Wikidata normalizer instance."""
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
