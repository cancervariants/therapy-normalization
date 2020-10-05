"""Test that the therapy normalizer works as intended."""
import pytest
from therapy.normalizers import Wikidata
from therapy.models import Drug
from therapy.normalizers.base import MatchType


@pytest.fixture(scope='module')
def wikidata():
    """Create a Wikidata normalizer instance."""
    w = Wikidata()
    return w


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'cisplatin',
        'concept_identifier': 'wikidata:Q412415',
        'aliases':
            list((
                'CDDP',
                'Cis-DDP',
                'CIS-DDP',
                'cis-diamminedichloroplatinum(II)',
                'Platinol',
                'Platinol-AQ'
            )),
        'other_identifiers':
            list((
                'chembl:CHEMBL11359',
                'rxcui:2555',
                'drugbank:00515',
                'chemidplus:15663-27-1',
                'pubchem.compound:5702198',
            )),
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def interferon_alfacon_1():
    """Create a Interferon alfacon-1 drug fixture"""
    params = {
        'label': 'Interferon alfacon-1',
        'concept_identifier': 'wikidata:Q15353101',
        'aliases':
            list((
                'CIFN',
                'consensus interferon',
                'IFN Alfacon-1',
                'Interferon Consensus, Methionyl',
                'methionyl interferon consensus',
                'methionyl-interferon-consensus',
                'rCon-IFN',
                'Recombinant Consensus Interferon',
                'Recombinant methionyl human consensus interferon',
            )),
        'other_identifiers':
            list((
                'chembl:CHEMBL1201557',
                'rxcui:59744',
                'chemidplus:118390-30-0',
                'drugbank:00069',
            )),
    }
    return Drug(**params)


def test_case_cisplatin_normalize(cisplatin, wikidata):
    """Test that cisplatin term normalizes to correct drug concept as
    PRIMARY, CASE INSENSITIVE PRIMARY, ALIAS, and CASE INSENSITIVE ALIAS
    matches.
    """
    normalizer_response = wikidata.normalize('cisplatin')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)

    normalizer_response = wikidata.normalize('Cisplatin')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY

    normalizer_response = wikidata.normalize('CDDP')
    assert normalizer_response.match_type == MatchType.ALIAS

    normalizer_response = wikidata.normalize('cddp')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_ALIAS


def test_case_interferon_alfacon_1_normalize(interferon_alfacon_1, wikidata):
    """Test that Interferon alfacon-1 normalizes to correct drug concept."""
    normalizer_response = wikidata.normalize('Interferon alfacon-1')
    assert normalizer_response.match_type == MatchType.PRIMARY
    assert len(normalizer_response.records) == 1
    normalized_drug = normalizer_response.records[0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier == \
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) ==\
           set(interferon_alfacon_1.other_identifiers)

    normalizer_response = wikidata.normalize('Interferon Alfacon-1')
    assert normalizer_response.match_type == MatchType.CASE_INSENSITIVE_PRIMARY

    normalizer_response = wikidata.normalize('Interferon alfacon - 1')
    assert normalizer_response.match_type == MatchType.NO_MATCH


def test_case_no_match(wikidata):
    """Test that term normalizes to NO match"""
    normalizer_response = wikidata.normalize('cisplati')
    assert normalizer_response.match_type == MatchType.NO_MATCH
    assert len(normalizer_response.records) == 0


def test_case_empty_query(wikidata):
    """Test that empty query normalizes to NO match"""
    normalizer_response = wikidata.normalize('')
    assert normalizer_response.match_type == MatchType.NO_MATCH
    assert len(normalizer_response.records) == 0
