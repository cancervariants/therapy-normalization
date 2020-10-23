"""Test that the therapy normalizer works as intended for the ChEMBL source."""
import pytest
from therapy.schemas import Drug, MatchType
from therapy import query


@pytest.fixture(scope='module')
def chembl():
    """Build ChEMBL test fixture."""
    class QueryGetter:
        def normalize(self, query_str, incl=''):
            resp = query.normalize(query_str, keyed=True, incl=incl)
            return resp['source_matches']['ChEMBL']

    c = QueryGetter()
    return c


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
    """Create a L-745870 drug fixture."""
    params = {
        'label': 'L-745870',
        'concept_identifier': 'chembl:CHEMBL267014',
        'aliases': list(('L-745870',)),
        'other_identifiers': list(),
        'max_phase': 0,
        'trade_name': list()
    }
    return Drug(**params)


def test_case_sensitive_primary(cisplatin, chembl):
    """Test that cisplatin drug normalizes to correct drug concept
    as a PRIMARY match.
    """
    normalizer_response = chembl.normalize('CISPLATIN')
    assert normalizer_response['match_type'] == MatchType.PRIMARY
    assert len(normalizer_response['records']) == 2
    normalized_drug = normalizer_response['records'][0]
    for alias in normalized_drug.aliases:
        assert alias in cisplatin.aliases
    assert len(normalized_drug.aliases) == len(cisplatin.aliases)
    assert normalized_drug.trade_name == cisplatin.trade_name
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('chembl:CHEMBL11359')
    assert normalizer_response['match_type'] == MatchType.PRIMARY
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    normalizer_response = chembl.normalize('CHEMBL11359')
    assert normalizer_response['match_type'] == MatchType.PRIMARY
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_insensitive(cisplatin, chembl):
    """Test that cisplatin drug normalizes to correct drug concept
    as CASE_INSENSITIVE_PRIMARY and NAMESPACE_CASE_INSENSITIVE matches.
    """
    # Test CASE_INSENSITIVE_PRIMARY
    normalizer_response = chembl.normalize('cisplatin')
    assert normalizer_response['match_type'] ==\
           MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response['records']) == 2
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    # Test CASE_INSENSITIVE_PRIMARY
    normalizer_response = chembl.normalize('chembl:chembl11359')
    assert normalizer_response['match_type'] == \
           MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    # Test CASE_INSENSITIVE_PRIMARY
    normalizer_response = chembl.normalize('cHEmbl:chembl11359')
    assert normalizer_response['match_type'] ==\
           MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier

    # Test NAMESPACE_CASE_INSENSITIVE
    normalizer_response = chembl.normalize('cHEmbl:CHEMBL11359')
    assert normalizer_response['match_type'] == \
           MatchType.NAMESPACE_CASE_INSENSITIVE
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_sensitive_alias(cisplatin, chembl):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = chembl.normalize('cis-diamminedichloroplatinum(II)')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_case_insensitive_alias(cisplatin, chembl):
    """Test that alias term normalizes to correct drug concept as an
    CASE_INSENSITIVE_ALIAS match.
    """
    normalizer_response = chembl.normalize('INT230-6 COMPONENT CISPLATIn')
    assert normalizer_response['match_type'] ==\
           MatchType.CASE_INSENSITIVE_ALIAS
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_identifier == cisplatin.concept_identifier


def test_no_match(chembl):
    """Test that a term normalizes to correct drug concept as a NO match."""
    normalizer_response = chembl.normalize('cisplati')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_query_with_symbols(l745870, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as PRIMARY, CASE_INSENSITIVE_PRIMARY and NO matches.
    """
    # Test PRIMARY
    normalizer_response = chembl.normalize('L-745870')
    assert normalizer_response['match_type'] == MatchType.PRIMARY
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_identifier == l745870.concept_identifier
    assert normalized_drug.aliases == l745870.aliases
    assert normalized_drug.concept_identifier == l745870.concept_identifier
    assert normalized_drug.max_phase == l745870.max_phase

    # Test CASE_INSENSITIVE_PRIMARY
    normalizer_response = chembl.normalize('l-745870')
    assert normalizer_response['match_type'] ==\
           MatchType.CASE_INSENSITIVE_PRIMARY
    assert len(normalizer_response['records']) == 1

    # Test NO_MATCH
    normalizer_response = chembl.normalize('L - 745870')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH


def test_case_empty_query(chembl):
    """Test that an empty normalizes to correct drug concept as a NO match."""
    normalizer_response = chembl.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_meta_info(cisplatin, chembl):
    """Test that the meta field is correct."""
    normalizer_response = chembl.normalize('cisplatin')
    assert normalizer_response['meta_'].data_license == 'CC BY-SA 3.0'
    assert normalizer_response['meta_'].data_license_url == \
           'https://creativecommons.org/licenses/by-sa/3.0/'
    assert normalizer_response['meta_'].version == '27'
    assert normalizer_response['meta_'].data_url == \
           'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'  # noqa: E501
