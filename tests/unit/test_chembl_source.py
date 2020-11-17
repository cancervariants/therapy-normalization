"""Test that the therapy normalizer works as intended for the ChEMBL source."""
import pytest
from therapy.schemas import Drug, MatchType
from therapy import query


@pytest.fixture(scope='module')
def chembl():
    """Build ChEMBL test fixture."""
    class QueryGetter:
        def normalize(self, query_str, incl='chembl'):
            resp = query.normalize(query_str, keyed=True, incl=incl)
            return resp['source_matches']['ChEMBL']

    c = QueryGetter()
    return c


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'CISPLATIN',
        'concept_id': 'chembl:CHEMBL11359',
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
        'approval_status': 'approved',
        'other_identifiers': list(),
        'trade_names': list((
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
        'concept_id': 'chembl:CHEMBL267014',
        'aliases': list(('L-745870',)),
        'approval_status': None,
        'other_identifiers': list(),
        'max_phase': 0,
        'trade_names': list()
    }
    return Drug(**params)


def test_concept_id_cisplatin(cisplatin, chembl):
    """Test that cisplatin drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = chembl.normalize('chembl:CHEMBL11359')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = chembl.normalize('CHEMBL11359')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = chembl.normalize('chembl:chembl11359')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = chembl.normalize('cHEmbl:chembl11359')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = chembl.normalize('cHEmbl:CHEMBL11359')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_primary_label_cisplatin(cisplatin, chembl):
    """Test that cisplatin drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = chembl.normalize('CISPLATIN')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 2
    if len(normalizer_response['records'][0].aliases) >\
            len(normalizer_response['records'][1].aliases):
        ind = 0
    else:
        ind = 1
    normalized_drug = normalizer_response['records'][ind]
    for alias in normalized_drug.aliases:
        assert alias in cisplatin.aliases
    assert len(normalized_drug.aliases) == len(cisplatin.aliases)
    for tn in normalized_drug.trade_names:
        assert tn in cisplatin.trade_names
    assert len(normalized_drug.trade_names) == len(cisplatin.trade_names)
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = chembl.normalize('cisplatin')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 2
    if len(normalizer_response['records'][0].aliases) >\
            len(normalizer_response['records'][1].aliases):
        ind = 0
    else:
        ind = 1
    normalized_drug = normalizer_response['records'][ind]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_alias_cisplatin(cisplatin, chembl):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = chembl.normalize('cis-diamminedichloroplatinum(II)')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = chembl.normalize('INT230-6 COMPONENT CISPLATIn')
    assert normalizer_response['match_type'] == \
           MatchType.ALIAS
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_no_match(chembl):
    """Test that a term normalizes to correct drug concept as a NO match."""
    normalizer_response = chembl.normalize('cisplati')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Test white space in between label
    normalizer_response = chembl.normalize('L - 745870')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    normalizer_response = chembl.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_concept_id_l745870(l745870, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = chembl.normalize('chembl:CHEMBL267014')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status

    normalizer_response = chembl.normalize('CHEMBL267014')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status

    normalizer_response = chembl.normalize('chembl:chembl267014')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status

    normalizer_response = chembl.normalize('cHEmbl:chembl267014')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status

    normalizer_response = chembl.normalize('cHEmbl:CHEMBL267014')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status


def test_primary_label_l745870(l745870, chembl):
    """Test that L-745870 drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = chembl.normalize('L-745870')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.aliases == l745870.aliases
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status

    normalizer_response = chembl.normalize('l-745870')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == l745870.label
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.aliases == l745870.aliases
    assert normalized_drug.concept_id == l745870.concept_id
    assert normalized_drug.approval_status == l745870.approval_status


def test_meta_info(cisplatin, chembl):
    """Test that the meta field is correct."""
    normalizer_response = chembl.normalize('cisplatin')
    assert normalizer_response['meta_'].data_license == 'CC BY-SA 3.0'
    assert normalizer_response['meta_'].data_license_url == \
           'https://creativecommons.org/licenses/by-sa/3.0/'
    assert normalizer_response['meta_'].version == '27'
    assert normalizer_response['meta_'].data_url == \
           'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'  # noqa: E501
