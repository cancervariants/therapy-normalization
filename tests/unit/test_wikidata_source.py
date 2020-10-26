"""Test that the therapy normalizer works as intended for the
Wikidata source.
"""
import pytest
from therapy import query
from therapy.schemas import Drug, MatchType


@pytest.fixture(scope='module')
def wikidata():
    """Build Wikidata test fixture."""
    class QueryGetter:
        def normalize(self, query_str, incl='wikidata'):
            resp = query.normalize(query_str, keyed=True, incl=incl)
            return resp['source_matches']['Wikidata']

    w = QueryGetter()
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
    """Create a Interferon alfacon-1 drug fixture."""
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


def test_concept_id_cisplatin(cisplatin, wikidata):
    """Test that cisplatin drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = wikidata.normalize('wikidata:Q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)

    normalizer_response = wikidata.normalize('wiKIdata:Q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)

    normalizer_response = wikidata.normalize('wiKIdata:q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)

    normalizer_response = wikidata.normalize('Q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)


def test_primary_label_cisplatin(cisplatin, wikidata):
    """Test that cisplatin drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = wikidata.normalize('cisplatin')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)

    normalizer_response = wikidata.normalize('Cisplatin')
    assert normalizer_response['match_type'] == \
           MatchType.PRIMARY_LABEL


def test_alias_cisplatin(cisplatin, wikidata):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = wikidata.normalize('CDDP')
    assert normalizer_response['match_type'] == MatchType.ALIAS

    normalizer_response = wikidata.normalize('cddp')
    assert normalizer_response['match_type'] ==\
           MatchType.ALIAS


def test_case_no_match(wikidata):
    """Test that a term normalizes to NO match."""
    normalizer_response = wikidata.normalize('cisplati')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Test white space in between label
    normalizer_response = wikidata.normalize('Interferon alfacon - 1')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    normalizer_response = wikidata.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_concept_id_interferon_alfacon_1(interferon_alfacon_1, wikidata):
    """Test that interferon alfacon-1 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = wikidata.normalize('wikidata:Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)

    normalizer_response = wikidata.normalize('wiKIdata:Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)

    normalizer_response = wikidata.normalize('wiKIdata:q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)

    normalizer_response = wikidata.normalize('Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)

    normalizer_response = wikidata.normalize('q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier == \
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)


def test_primary_label_interferon_alfacon_1(interferon_alfacon_1, wikidata):
    """Test that Interferon alfacon-1 drug normalizes to correct drug
    concept as a PRIMARY_LABEL match.
    """
    normalizer_response = wikidata.normalize('Interferon alfacon-1')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier == \
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) ==\
           set(interferon_alfacon_1.other_identifiers)

    normalizer_response = wikidata.normalize('Interferon Alfacon-1')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL


def test_meta_info(cisplatin, wikidata):
    """Test that the meta field is correct."""
    normalizer_response = wikidata.normalize('cisplatin')
    print(normalizer_response.keys())
    assert normalizer_response['meta_'].data_license == 'CC0 1.0'
    assert normalizer_response['meta_'].data_license_url == \
           'https://creativecommons.org/publicdomain/zero/1.0/'
    assert normalizer_response['meta_'].version == '20200812'
    assert normalizer_response['meta_'].data_url is None
