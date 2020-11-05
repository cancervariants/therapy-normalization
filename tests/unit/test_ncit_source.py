"""Test NCIT source"""
import pytest
from therapy.schemas import Drug, MatchType
from therapy import query


@pytest.fixture(scope='module')
def ncit():
    """Build NCIt test fixture"""
    class QueryGetter:
        def normalize(self, query_str, incl='chembl'):
            resp = query.normalize(query_str, keyed=True, incl=incl)
            return resp['source_matches']['NCIt']

    c = QueryGetter()
    return c


@pytest.fixture(scope='module')
def voglibose():
    """Create voglibose drug fixture"""
    params = {
        'label': 'Voglibose',
        'concept_identifier': 'ncit:C95221',
        'aliases': ['3,4-Dideoxy-4-((2-Hydroxy-1-(Hydroxymethyl)Ethyl)Amino)-2-C-(Hydroxymethyl)-D-Epi-Inositol',  # noqa F401
                    'A-71100', 'AO-128', 'Basen',
                    'N-(1,3-Dihydroxy-2-Propyl)Valiolamine', 'VOGLIBOSE'],
        'other_identifiers': ['chemidplus:83480-29-9', 'fda:S77P977AG8'],
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def apricoxib():
    """Create aprocoxib drug fixture"""
    params = {
        'label': 'Apricoxib',
        'concept_identifier': 'C74021',
        'aliases': ['APRICOXIB', 'COX-2 Inhibitor TG01', 'CS-706', 'R-109339',
                    'TG01', 'TP2001'],
        'other_identifiers': ['chemidplus:197904-84-0', 'fda:5x5HB3Vz3Z'],
    }
    return Drug(**params)


def test_concept_id_voglibose(voglibose, ncit):
    """Test that Voglibose successfully matches to concept ID queries"""
    response = ncit.normalize('ncit:C95221')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    drug = response['records'][0]
    assert drug.concept_identifier == voglibose.concept_identifier
    assert set(drug.aliases) == set(voglibose.aliases)
    assert set(drug.other_identifiers) == \
           set(voglibose.other_identifiers)
    assert drug.approval_status == voglibose.approval_status

    normalizer_response = ncit.normalize('NCIT:C95221')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    drug = normalizer_response['records'][0]
    assert drug.label == voglibose.label
    assert drug.concept_identifier == voglibose.concept_identifier
    assert set(drug.aliases) == set(voglibose.aliases)
    assert set(drug.other_identifiers) == \
           set(voglibose.other_identifiers)
    assert drug.approval_status == voglibose.approval_status

    normalizer_response = ncit.normalize('NCIt:c95221')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    drug = normalizer_response['records'][0]
    assert drug.label == voglibose.label
    assert drug.concept_identifier == voglibose.concept_identifier
    assert set(drug.aliases) == set(voglibose.aliases)
    assert set(drug.other_identifiers) == \
           set(voglibose.other_identifiers)
    assert drug.approval_status == voglibose.approval_status

    normalizer_response = ncit.normalize('C95221')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    drug = normalizer_response['records'][0]
    assert drug.label == voglibose.label
    assert drug.concept_identifier == voglibose.concept_identifier
    assert set(drug.aliases) == set(voglibose.aliases)
    assert set(drug.other_identifiers) == \
           set(voglibose.other_identifiers)
    assert drug.approval_status == voglibose.approval_status


def test_primary_label_voglibose(voglibose, ncit):
    """Test that voglibose drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = ncit.normalize('voglibose')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == voglibose.label
    assert normalized_drug.concept_identifier == voglibose.concept_identifier
    assert set(normalized_drug.aliases) == set(voglibose.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(voglibose.other_identifiers)
    assert normalized_drug.approval_status == voglibose.approval_status

    normalizer_response = ncit.normalize('voglibose')
    assert normalizer_response['match_type'] == \
           MatchType.PRIMARY_LABEL


def test_alias_voglibose(voglibose, ncit):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = ncit.normalize('VOGLIBOSE')
    assert normalizer_response['match_type'] == MatchType.ALIAS

    normalizer_response = ncit.normalize('A0-128')
    assert normalizer_response['match_type'] ==\
           MatchType.ALIAS


def test_case_no_match(ncit):
    """Test that a term normalizes to NO match."""
    normalizer_response = ncit.normalize('voglibo')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Test white space in between label
    normalizer_response = ncit.normalize('Volgibo')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    normalizer_response = ncit.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_concept_id_interferon_alfacon_1(interferon_alfacon_1, ncit):
    """Test that interferon alfacon-1 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = ncit.normalize('ncit:Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = ncit.normalize('ncit:Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = ncit.normalize('ncit:q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = ncit.normalize('Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier ==\
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = ncit.normalize('q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier == \
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status


def test_primary_label_interferon_alfacon_1(interferon_alfacon_1, ncit):
    """Test that Interferon alfacon-1 drug normalizes to correct drug
    concept as a PRIMARY_LABEL match.
    """
    normalizer_response = ncit.normalize('Interferon alfacon-1')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_identifier == \
           interferon_alfacon_1.concept_identifier
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) ==\
           set(interferon_alfacon_1.other_identifiers)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = ncit.normalize('Interferon Alfacon-1')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL


def test_meta_info(voglibose, ncit):
    """Test that the meta field is correct."""
    normalizer_response = ncit.normalize('voglibose')
    assert normalizer_response['meta_'].data_license == 'CC BY 4.0'
    assert normalizer_response['meta_'].data_license_url == \
        'https://creativecommons.org/licenses/by/4.0/legalcode'
    assert normalizer_response['meta_'].version == '20.09d'
    assert normalizer_response['meta_'].data_url == \
        "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/Thesaurus_20.09d.OWL.zip"  # noqa F401
