"""Test that the therapy normalizer works as intended for the DrugBank
source.
"""
import pytest
from therapy.schemas import Drug, MatchType
from therapy import query


@pytest.fixture(scope='module')
def drugbank():
    """Build DrugBank test fixture."""
    class QueryGetter:
        def normalize(self, query_str, incl='drugbank'):
            resp = query.normalize(query_str, keyed=True, incl=incl)
            return resp['source_matches']['DrugBank']

    d = QueryGetter()
    return d


@pytest.fixture(scope='module')
def lepirudin():
    """Create a Lepirudin drug fixture."""
    params = {
        'label': 'Lepirudin',
        'concept_identifier': 'drugbank:DB00001',
        'aliases': list((
            'Hirudin variant-1',
            'Lepirudin recombinant',
            'BTD00024',
            'BIOD00024'
        )),
        'other_identifiers': list((
            'chemidplus:138068-37-8',
        )),
        'trade_name': list((
            'Refludan',
        ))
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def db14201():
    """Create a db14201 drug fixture."""
    params = {
        'label': '2,2\'-Dibenzothiazyl disulfide',
        'concept_identifier': 'drugbank:DB14201',
        'aliases': list((
            '2-Benzothiazolyl disulfide',
            '2-Mercaptobenzothiazole disulfide',
            '2,2\'-Benzothiazyl disulfide',
            '2,2\'-Bis(benzothiazolyl) disulfide',
            '2,2\'-Dithiobisbenzothiazole',
            'Benzothiazole disulfide',
            'Benzothiazolyl disulfide',
            'Bis(2-benzothiazolyl) disulfide',
            'Bis(2-benzothiazyl) disulfide',
            'Bis(benzothiazolyl) disulfide',
            'BTS-SBT',
            'DBTD',
            'di(1,3-benzothiazol-2-yl) disulfide',
            'dibenzothiazol-2-yl disulfide',
            'Dibenzothiazolyl disulfide',
            'Dibenzothiazolyl disulphide',
            'Dibenzothiazyl disulfide',
            'Dibenzoylthiazyl disulfide',
            'Thiofide'
        )),
        'other_identifiers': list((
            'chemidplus:120-78-5',
        )),
        'trade_name': list((
            'T.R.U.E. Test Thin-Layer Rapid Use Patch Test',
        ))
    }
    return Drug(**params)


def test_concept_id_lepirudin(lepirudin, drugbank):
    """Test that lepirudin drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = drugbank.normalize('drugbank:DB00001')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('DB00001')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('drugbank:db00001')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('Drugbank:db00001')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('druGBank:DB00001')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers


def test_primary_label_lepirudin(lepirudin, drugbank):
    """Test that lepirudin drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = drugbank.normalize('LEPIRUDIN')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    for alias in normalized_drug.aliases:
        assert alias in lepirudin.aliases
    assert len(normalized_drug.aliases) == len(lepirudin.aliases)
    assert normalized_drug.trade_name == lepirudin.trade_name
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('lepirudin')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers


def test_alias_lepirudin(lepirudin, drugbank):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = drugbank.normalize('Hirudin variant-1')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('BTD00024')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers

    normalizer_response = drugbank.normalize('BIOD00024')
    assert normalizer_response['match_type'] == \
           MatchType.ALIAS
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_identifier == lepirudin.concept_identifier
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers


def test_concept_id_db14201(db14201, drugbank):
    """Test that db14201 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = drugbank.normalize('drugbank:DB14201')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('DB14201')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('drugbank:db14201')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('Drugbank:db14201')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('druGBank:DB14201')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers


def test_primary_label_db14201(db14201, drugbank):
    """Test that db14201 drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = drugbank.normalize('2,2\'-Dibenzothiazyl disulfide')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    for alias in normalized_drug.aliases:
        assert alias in db14201.aliases
    assert len(normalized_drug.aliases) == len(db14201.aliases)
    assert normalized_drug.trade_name == db14201.trade_name
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('2,2\'-Dibenzothiazyl Disulfide')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers


def test_alias_db14201(db14201, drugbank):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = drugbank.normalize('benzothiazole disulfide')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('BTS-SBT')
    assert normalizer_response['match_type'] == \
           MatchType.ALIAS
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers

    normalizer_response = drugbank.normalize('THIOFIDE')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_identifier == db14201.concept_identifier
    assert normalized_drug.other_identifiers == db14201.other_identifiers


def test_no_match(drugbank):
    """Test that a term normalizes to correct drug concept as a NO match."""
    normalizer_response = drugbank.normalize('lepirudi')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Polish alias for DB14201
    normalizer_response = drugbank.normalize('Dwusiarczek dwubenzotiazylu')
    assert normalizer_response['match_type'] == \
           MatchType.NO_MATCH

    # Test white space in between id
    normalizer_response = drugbank.normalize('DB 00001')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    normalizer_response = drugbank.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_meta_info(lepirudin, drugbank):
    """Test that the meta field is correct."""
    normalizer_response = drugbank.normalize('lepirudin')
    assert normalizer_response['meta_'].data_license == 'CC BY-NC 4.0'
    assert normalizer_response['meta_'].data_license_url == \
           'https://creativecommons.org/licenses/by-nc/4.0/legalcode'
    assert normalizer_response['meta_'].version == '5.1.7'
    assert normalizer_response['meta_'].data_url == \
           'https://go.drugbank.com/releases/5-1-7/downloads/all-full-database'  # noqa: E501
