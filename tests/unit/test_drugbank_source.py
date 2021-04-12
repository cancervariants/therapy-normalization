"""Test that the therapy normalizer works as intended for the DrugBank
source.
"""
import pytest
from therapy.schemas import Drug, MatchType
from therapy.query import QueryHandler
from tests.conftest import compare_records


@pytest.fixture(scope='module')
def drugbank():
    """Build DrugBank normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search(self, query_str):
            resp = self.query_handler.search_sources(query_str, keyed=True,
                                                     incl='drugbank')
            return resp['source_matches']['DrugBank']

        def fetch_meta(self):
            return self.query_handler._fetch_meta('DrugBank')

    return QueryGetter()


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
            'Platin'
        ],
        'approval_status': 'approved',
        'other_identifiers': [
            'rxcui:2555',
            'chembl:CHEMBL2068237',
            'chemidplus:15663-27-1',
        ],
        'xrefs': [
            'chebi:27899',
            'pubchem.compound:2767',
            'pubchem.substance:46504561',
            'kegg.compound:C06911',
            'kegg.drug:D00275',
            'chemspider:76401',
            'bindingdb:50028111',
            'pharmgkb.drug:PA449014',
            'pdb:CPT',
            'ttd:DAP000215',
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
            'Platinol-AQ',
            'PMS-cisplatin'
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
            'rxcui:18896',
            'chemidplus:37106-97-1',
            'chembl:CHEMBL1200368'
        ],
        'xrefs': [
            'chebi:31263',
            'pubchem.compound:6957673',
            'pubchem.substance:46508175',
            'kegg.drug:D01346',
            'chemspider:5329364',
            'bindingdb:50240073',
            'pharmgkb.drug:PA164750572',
            'zinc:ZINC000000608204',
        ],
        'trade_names': []
    }
    return Drug(**params)


# Tests filtering on aliases and trade_names length
@pytest.fixture(scope='module')
def db14201():
    """Create a db14201 drug fixture."""
    params = {
        'label': '2,2\'-Dibenzothiazyl disulfide',
        'concept_id': 'drugbank:DB14201',
        'aliases': [],
        'approval_status': 'approved',
        'other_identifiers': [
            'rxcui:1306112',
            'chemidplus:120-78-5',
            'chembl:CHEMBL508112'
        ],
        'xrefs': [
            'chemspider:8139',
            'bindingdb:50444458',
            'chebi:53239',
            'zinc:ZINC000001555224',
        ],
        'trade_names': [
            'T.R.U.E. Test Thin-Layer Rapid Use Patch Test'
        ]
    }
    return Drug(**params)


def test_cisplatin(cisplatin, drugbank):
    """Test that cisplatin drug normalizes to correct drug concept."""
    response = drugbank.search('drugbank:DB00515')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('DB00515')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('drugbank:db00515')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('Drugbank:db00515')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('druGBank:DB00515')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('cisplatin')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('cisplatin')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('Abiplatin')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('Cis-ddp')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('Platidiam')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('Platinol')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('platinol-aq')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('pms-cisplatin')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('chembl:chembl2068237')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('rxcui:2555')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = drugbank.search('pubchem.substance:46504561')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)


def test_bentiromide(bentiromide, drugbank):
    """Test that bentiromide drug normalizes to correct drug concept."""
    response = drugbank.search('drugbank:DB00522')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('DB00522')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('drugbank:db00522')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('Drugbank:db00522')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('druGBank:DB00522')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('Bentiromide')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('bentiromide')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('APRD00818')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('pfd')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('PFT')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('chemidplus:37106-97-1')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)

    response = drugbank.search('pharmgkb.drug:PA164750572')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], bentiromide)


def test_db14201(db14201, drugbank):
    """Test that db14201 drug normalizes to correct drug concept."""
    response = drugbank.search('drugbank:DB14201')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('DB14201')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('drugbank:db14201')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('Drugbank:db14201')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('druGBank:DB14201')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search("2,2'-Dibenzothiazyl disulfide")
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('2,2\'-dibenzothiazyl disulfide')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('rxcui:1306112')
    assert response['match_type'] == MatchType.OTHER_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = drugbank.search('chebi:53239')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], db14201)

    response = \
        drugbank.search('T.R.U.E. Test Thin-Layer Rapid Use Patch Test')
    assert response['match_type'] == MatchType.TRADE_NAME


def test_no_match(drugbank):
    """Test that a term normalizes to correct drug concept as a NO match."""
    response = drugbank.search('lepirudi')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0

    # Polish alias for DB14201
    response = drugbank.search('Dwusiarczek dwubenzotiazylu')
    assert response['match_type'] == \
           MatchType.NO_MATCH

    # Test white space in between id
    response = drugbank.search('DB 00001')
    assert response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    response = drugbank.search('')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0


def test_meta_info(drugbank):
    """Test that the meta field is correct."""
    response = drugbank.fetch_meta()
    assert response.data_license == 'CC BY-NC 4.0'
    assert response.data_license_url == \
           'https://creativecommons.org/licenses/by-nc/4.0/legalcode'
    assert response.version == '5.1.7'
    assert response.data_url == \
           'https://go.drugbank.com/releases/5-1-7/downloads/all-full-database'  # noqa: E501
    assert response.rdp_url == 'http://reusabledata.org/drugbank.html'  # noqa: E501
    assert response.data_license_attributes == {
        "non_commercial": True,
        "share_alike": False,
        "attribution": True
    }
