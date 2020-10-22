"""Test that the chEMBL therapy normalizer works as intended."""
import pytest
from therapy.schemas import Drug
from therapy.schemas import MatchType
from therapy.query import normalize


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'CISPLATIN',
        'concept_identifier': 'chembl:CHEMBL11359',
        'aliases': list((
            'Cisplatin',
            'Cis-Platinum II',
            'Cisplatinum',
            'cis-diamminedichloroplatinum(II)',
            'CIS-DDP',
            'INT-230-6 COMPONENT CISPLATIN',
            'INT230-6 COMPONENT CISPLATIN',
            'NSC-119875',
            'Platinol',
            'Platinol-Aq',
        )),
        'other_identifiers': list(),
        'trade_name': list((
            'PLATINOL',
            'PLATINOL-AQ',
            'CISPLATIN'
        ))
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
        'trade_name': list()
    }
    return Drug(**params)


def test_case_sensitive_primary(cisplatin):
    """Test that cisplatin term normalizes to correct drug concept
    as a PRIMARY match.
    """
    normalizer_response = normalize('CISPLATIN', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.PRIMARY
    assert len(chembl_dict['records']) == 2
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.aliases == cisplatin.aliases
    assert normalized_drug.trade_name == cisplatin.trade_name
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = normalize('chembl:CHEMBL11359', incl='chembl',
                                    keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.PRIMARY
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = normalize('CHEMBL11359', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.PRIMARY
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_insensitive_primary(cisplatin):
    """Tests that cisplatin term normalizes to correct drug concept
    as CASE_INSENSITIVE_PRIMARY, NAMESPACE_CASE_INSENSITIVE matches.
    """
    normalizer_response = normalize('cisplatin', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(chembl_dict['records']) == 2
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = normalize('chembl:chembl11359', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = normalize('cHEmbl:CHEMBL11359', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == \
           MatchType.NAMESPACE_CASE_INSENSITIVE
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = normalize('cHEmbl:chembl11359', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_sensitive_alias(cisplatin):
    """Tests that alias term normalizes correctly"""
    normalizer_response = normalize('cis-diamminedichloroplatinum(II)',
                                    keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.ALIAS
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_insensitive_alias(cisplatin):
    """Tests that case-insensitive alias term normalizes correctly"""
    normalizer_response = normalize('INT230-6 COMPONENT CISPLATIn', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.CASE_INSENSITIVE_ALIAS
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_no_match():
    """Test that term normalizes to NO match"""
    normalizer_response = normalize('cisplati', keyed=True, incl='chembl')
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.NO_MATCH
    assert len(chembl_dict['records']) == 0


def test_query_with_symbols(l745870):
    """Test that L-745870 normalizes to PRIMARY and CASE_INSENSITIVE match"""
    normalizer_response = normalize('L-745870', keyed=True, incl='chembl')
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.PRIMARY
    assert len(chembl_dict['records']) == 1
    normalized_drug = chembl_dict['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_identifier == l745870.concept_identifier
    assert normalized_drug.aliases == l745870.aliases
    assert normalized_drug.concept_identifier == l745870.concept_identifier
    assert normalized_drug.max_phase == l745870.max_phase

    normalizer_response = normalize('l-745870', keyed=True, incl='chembl')
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(chembl_dict['records']) == 1

    normalizer_response = normalize('L - 745870', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.NO_MATCH


def test_case_empty_query():
    """Test that empty query normalizes to NO match"""
    normalizer_response = normalize('', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['match_type'] == MatchType.NO_MATCH
    assert len(chembl_dict['records']) == 0


def test_meta_info(cisplatin):
    """Test that the meta field is correct."""
    normalizer_response = normalize('cisplatin', keyed=True)
    chembl_dict = normalizer_response['source_matches']['ChEMBL']
    assert chembl_dict['meta_'].data_license == 'CC BY-SA 3.0'
    assert chembl_dict['meta_'].data_license_url == \
           'https://creativecommons.org/licenses/by-sa/3.0/'
    assert chembl_dict['meta_'].version == '27'
    assert chembl_dict['meta_'].data_url == \
           'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'  # noqa: E501
