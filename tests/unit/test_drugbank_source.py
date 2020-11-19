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
        'concept_id': 'drugbank:DB00001',
        'aliases': list((
            'Hirudin variant-1',
            'Lepirudin recombinant',
            'BTD00024',
            'BIOD00024'
        )),
        'approval_status': 'approved',
        'other_identifiers': list((
            'chemidplus:138068-37-8',
            'pubchem.substance:46507011',
            'kegg.drug:D06880',
            'pharmgkb:PA450195',
            'chembl:CHEMBL1201666',
            'rxcui:237057',
        )),
        'trade_names': list((
            'Refludan',
        ))
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def protoporphyrin():
    """Create a Protoporphyrin fixture"""
    params = {
        'label': 'Protoporphyrin Ix Containing Zn',
        'concept_id': 'drugbank:DB03934',
        'aliases': ['EXPT03292'],
        'other_identifiers': [
            'chebi:28783',
            'pubchem.compound:3802528',
            'pubchem.substance:46506175',
            'kegg.compound:C03184'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def db14201():
    """Create a db14201 drug fixture."""
    params = {
        'label': "2,2'-Dibenzothiazyl disulfide",
        'concept_id': 'drugbank:DB14201',
        'aliases': list((
            # *** removed pending resolution of handling high alias counts ***
            # '2-Benzothiazolyl disulfide',
            # '2-Mercaptobenzothiazole disulfide',
            # "2,2'-Benzothiazyl disulfide",
            # "2,2'-Bis(benzothiazolyl) disulfide",
            # "2,2'-Dithiobisbenzothiazole",
            # 'Benzothiazole disulfide',
            # 'Benzothiazolyl disulfide',
            # 'Bis(2-benzothiazolyl) disulfide',
            # 'Bis(2-benzothiazyl) disulfide',
            # 'Bis(benzothiazolyl) disulfide',
            # 'BTS-SBT',
            # 'DBTD',
            # 'di(1,3-benzothiazol-2-yl) disulfide',
            # 'dibenzothiazol-2-yl disulfide',
            # 'Dibenzothiazolyl disulfide',
            # 'Dibenzothiazolyl disulphide',
            # 'Dibenzothiazyl disulfide',
            # 'Dibenzoylthiazyl disulfide',
            # 'Thiofide'
        )),
        'approval_status': 'approved',
        'other_identifiers': list((
            'chemidplus:120-78-5',
            'chebi:53239',
            'bindingdb:50444458',
            'chembl:CHEMBL508112',
            'rxcui:1306112',
        )),
        'trade_names': list((
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
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('DB00001')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('drugbank:db00001')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('Drugbank:db00001')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('druGBank:DB00001')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status


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
    assert normalized_drug.trade_names == lepirudin.trade_names
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('lepirudin')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status


def test_alias_lepirudin(lepirudin, drugbank):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = drugbank.normalize('Hirudin variant-1')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('BTD00024')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status

    normalizer_response = drugbank.normalize('BIOD00024')
    assert normalizer_response['match_type'] == \
           MatchType.ALIAS
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lepirudin.label
    assert normalized_drug.concept_id == lepirudin.concept_id
    assert normalized_drug.other_identifiers == lepirudin.other_identifiers
    assert normalized_drug.approval_status == lepirudin.approval_status


def test_concept_id_db14201(db14201, drugbank):
    """Test that db14201 drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = drugbank.normalize('drugbank:DB14201')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status

    normalizer_response = drugbank.normalize('DB14201')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status

    normalizer_response = drugbank.normalize('drugbank:db14201')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status

    normalizer_response = drugbank.normalize('Drugbank:db14201')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status

    normalizer_response = drugbank.normalize('druGBank:DB14201')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status


def test_primary_label_db14201(db14201, drugbank):
    """Test that db14201 drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = drugbank.normalize("2,2'-Dibenzothiazyl disulfide")
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    for alias in normalized_drug.aliases:
        assert alias in db14201.aliases
    assert len(normalized_drug.aliases) == len(db14201.aliases)
    assert normalized_drug.trade_names == db14201.trade_names
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status

    normalizer_response = drugbank.normalize("2,2'-Dibenzothiazyl Disulfide")
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == db14201.label
    assert normalized_drug.concept_id == db14201.concept_id
    assert normalized_drug.other_identifiers == db14201.other_identifiers
    assert normalized_drug.approval_status == db14201.approval_status


def test_alias_protoporphyrin(protoporphyrin, drugbank):
    """Test that alias term normalizes correctly"""
    normalizer_response = drugbank.normalize('EXPT03292')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == protoporphyrin.label
    assert normalized_drug.concept_id == protoporphyrin.concept_id
    assert normalized_drug.other_identifiers ==\
        protoporphyrin.other_identifiers
    assert normalized_drug.approval_status == protoporphyrin.approval_status


# def test_alias_db14201(db14201, drugbank):
#     """Test that alias term normalizes to correct drug concept as an
#     ALIAS match.
#     """
#     normalizer_response = drugbank.normalize('benzothiazole disulfide')
#     assert normalizer_response['match_type'] == MatchType.ALIAS
#     assert len(normalizer_response['records']) == 1
#     normalized_drug = normalizer_response['records'][0]
#     assert normalized_drug.label == db14201.label
#     assert normalized_drug.concept_id == db14201.concept_id
#     assert normalized_drug.other_identifiers == db14201.other_identifiers
#     assert normalized_drug.approval_status == db14201.approval_status
#
#     normalizer_response = drugbank.normalize('BTS-SBT')
#     assert normalizer_response['match_type'] == \
#            MatchType.ALIAS
#     normalized_drug = normalizer_response['records'][0]
#     assert normalized_drug.label == db14201.label
#     assert normalized_drug.concept_id == db14201.concept_id
#     assert normalized_drug.other_identifiers == db14201.other_identifiers
#     assert normalized_drug.approval_status == db14201.approval_status
#
#     normalizer_response = drugbank.normalize('THIOFIDE')
#     assert normalizer_response['match_type'] == MatchType.ALIAS
#     assert len(normalizer_response['records']) == 1
#     normalized_drug = normalizer_response['records'][0]
#     assert normalized_drug.label == db14201.label
#     assert normalized_drug.concept_id == db14201.concept_id
#     assert normalized_drug.other_identifiers == db14201.other_identifiers
#     assert normalized_drug.approval_status == db14201.approval_status


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
