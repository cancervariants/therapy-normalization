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

        def fetch_meta(self):
            return query.fetch_meta('DrugBank')

    d = QueryGetter()
    return d


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'Cisplatin',
        'concept_id': 'drugbank:DB00515',
        'aliases': [
            'CDDP',
            'Cis-DDP',
            'cis-diamminedichloroplatinum(II)',
            'Cisplatin',
            'APRD00359',
            # International / Other brands
            'Abiplatin',
            'Cisplatyl',
            'Platidiam',
            'Platin',
            # Generic Products
            'CISplatin',
            'PMS-cisplatin'
        ],
        'approval_status': 'approved',
        'other_identifiers': [
            'chemidplus:15663-27-1',
            'chebi:27899',
            'pubchem.compound:2767',
            'pubchem.substance:46504561',
            'kegg.compound:C06911',
            'kegg.drug:D00275',
            'chemspider:76401',
            'bindingdb:50028111',
            'pharmgkb:PA449014',
            'pdb:CPT',
            'ttd:DAP000215',
            'chembl:CHEMBL2068237',
            'rxcui:2555'
        ],
        'trade_names': [
            'CISplatin',
            'Cisplatin',
            'Cisplatin Inj 0.5mg/ml',
            'Cisplatin Inj 1mg/ml',
            'Cisplatin Injection',
            'Cisplatin Injection BP',
            'Cisplatin Injection, BP',
            'Cisplatin Injection, Mylan Std.',
            'Platinol',
            'Platinol AQ Inj 1mg/ml',
            'Platinol-AQ'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def bentiromide():
    """Create a bentiromide drug fixture."""
    params = {
        'label': 'Bentiromide',
        'concept_id': 'drugbank:DB00522',
        'aliases': [
            'APRD00818',
            'Bentiromide',
            'BTPABA',
            'PFT',
            'PFD'
        ],
        'approval_status': 'withdrawn',
        'other_identifiers': [
            'chemidplus:37106-97-1',
            'chebi:31263',
            'pubchem.compound:6957673',
            'pubchem.substance:46508175',
            'kegg.drug:D01346',
            'chemspider:5329364',
            'bindingdb:50240073',
            'pharmgkb:PA164750572',
            'chembl:CHEMBL1200368',
            'zinc:ZINC000000608204',
            'rxcui:18896'
        ],
        'trade_name': [
        ]
    }
    return Drug(**params)


def test_cisplatin_concept_id(cisplatin, drugbank):
    """Test that cisplatin drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = drugbank.normalize('drugbank:DB00515')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('DB00515')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('drugbank:db00515')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('Drugbank:db00515')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('druGBank:DB00515')
    assert normalizer_response['match_type'] == \
           MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_cisplatin_label(cisplatin, drugbank):
    """Test that cisplatin drug normalizes to correct drug concept
    as a PRIMARY_LABEL match.
    """
    normalizer_response = drugbank.normalize('cisplatin')
    assert normalizer_response['match_type'] == MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('cisplatin')
    assert normalizer_response['match_type'] ==\
           MatchType.PRIMARY_LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_cisplatin_alias(cisplatin, drugbank):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = drugbank.normalize('Abiplatin')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('Cis-ddp')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('Platidiam')
    assert normalizer_response['match_type'] == \
           MatchType.ALIAS
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_cisplatin_trade_name(cisplatin, drugbank):
    """Test that alias term normalizes to correct drug concept as an
    TRADE_NAME match.
    """
    normalizer_response = drugbank.normalize('Platinol')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('platinol-aq')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = drugbank.normalize('CISplatin')
    assert normalizer_response['match_type'] == \
           MatchType.TRADE_NAME
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status


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


def test_meta_info(cisplatin, drugbank):
    """Test that the meta field is correct."""
    normalizer_response = drugbank.fetch_meta()
    assert normalizer_response.data_license == 'CC BY-NC 4.0'
    assert normalizer_response.data_license_url == \
           'https://creativecommons.org/licenses/by-nc/4.0/legalcode'
    assert normalizer_response.version == '5.1.7'
    assert normalizer_response.data_url == \
           'https://go.drugbank.com/releases/5-1-7/downloads/all-full-database'  # noqa: E501
