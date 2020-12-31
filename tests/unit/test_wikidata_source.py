"""Test that the therapy normalizer works as intended for the
Wikidata source.
"""
import pytest
from therapy.schemas import Drug, MatchType
from therapy.query import Normalizer


@pytest.fixture(scope='module')
def wikidata():
    """Build Wikidata normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = Normalizer()

        def normalize(self, query_str):
            resp = self.normalizer.normalize(query_str, keyed=True,
                                             incl='wikidata')
            return resp['source_matches']['Wikidata']
    return QueryGetter()


@pytest.fixture(scope='module')
def cisplatin():
    """Create a cisplatin drug fixture."""
    params = {
        'label': 'cisplatin',
        'concept_id': 'wikidata:Q412415',
        'aliases': [
            'CDDP',
            'Cis-DDP',
            'CIS-DDP',
            'cis-diamminedichloroplatinum(II)',
            'Platinol',
            'Platinol-AQ'
        ],
        'approval_status': None,
        'other_identifiers': [
            'chembl:CHEMBL11359',
            'drugbank:DB00515'
        ],
        'xrefs': [
            'rxcui:2555',
            'chemidplus:15663-27-1',
            'pubchem.compound:5702198'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def interferon_alfacon_1():
    """Create an Interferon alfacon-1 drug fixture."""
    params = {
        'label': 'Interferon alfacon-1',
        'concept_id': 'wikidata:Q15353101',
        'aliases': [
            'CIFN',
            'consensus interferon',
            'IFN Alfacon-1',
            'Interferon Consensus, Methionyl',
            'methionyl interferon consensus',
            'methionyl-interferon-consensus',
            'rCon-IFN',
            'Recombinant Consensus Interferon',
            'Recombinant methionyl human consensus interferon'
        ],
        'approval_status': None,
        'other_identifiers': [
            'chembl:CHEMBL1201557',
            'drugbank:DB00069'
        ],
        'xrefs': [
            'rxcui:59744',
            'chemidplus:118390-30-0'
        ],
        'trade_names': []
    }
    return Drug(**params)


# Test drug that has > 20 aliases
@pytest.fixture(scope='module')
def d_methamphetamine():
    """Create a D-methamphetamine drug fixture."""
    params = {
        'label': 'D-methamphetamine',
        'concept_id': 'wikidata:Q191924',
        'aliases': [],
        'approval_status': None,
        'other_identifiers': [
            'chembl:CHEMBL1201201',
            'drugbank:DB01577'
        ],
        'xrefs': [
            'chemidplus:537-46-2',
            'pubchem.compound:10836',
            'rxcui:6816'
        ],
        'trade_names': []
    }
    return Drug(**params)


# Test removing duplicate (case sensitive) aliases
@pytest.fixture(scope='module')
def atropine():
    """Create an atropine drug fixture."""
    params = {
        'label': 'atropine',
        'concept_id': 'wikidata:Q26272',
        'aliases': [
            'dl-Hyoscyamine',
            'dl-tropyltropate',
            'Mydriasine',
            'Tropine tropate',
            '(±)-atropine',
            '(±)-hyoscyamine',
            '(+-)-Atropine',
            '(+-)-Hyoscyamine',
            '(+,-)-Tropyl tropate',
            '(3-Endo)-8-methyl-8-azabicyclo[3.2.1]oct-3-yl tropate',
            '[(1S,5R)-8-Methyl-8-azabicyclo[3.2.1]oct-3-yl] '
            '3-hydroxy-2-phenyl-propanoate',
            '8-Methyl-8-azabicyclo[3.2.1]oct-3-yl '
            '3-hydroxy-2-phenylpropanoate',
            '8-Methyl-8-azabicyclo[3.2.1]oct-3-yl tropate'
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'rxcui:1223'
        ],
        'trade_names': []
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
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:Q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = wikidata.normalize('Q412415')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_primary_label_cisplatin(cisplatin, wikidata):
    """Test that cisplatin drug normalizes to correct drug concept
    as a LABEL match.
    """
    normalizer_response = wikidata.normalize('cisplatin')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = wikidata.normalize('Cisplatin')
    assert normalizer_response['match_type'] == \
           MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_alias_cisplatin(cisplatin, wikidata):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = wikidata.normalize('CDDP')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = wikidata.normalize('cddp')
    assert normalizer_response['match_type'] ==\
           MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert normalized_drug.approval_status == cisplatin.approval_status


def test_concept_id_atropine(atropine, wikidata):
    """Test that atropine drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = wikidata.normalize('wikidata:Q26272')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:Q26272')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:q26272')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status

    normalizer_response = wikidata.normalize('Q26272')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status


def test_primary_label_atropine(atropine, wikidata):
    """Test that atropine drug normalizes to correct drug concept
    as a LABEL match.
    """
    normalizer_response = wikidata.normalize('atropine')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status

    normalizer_response = wikidata.normalize('Atropine')
    assert normalizer_response['match_type'] == \
           MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status


def test_alias_atropine(atropine, wikidata):
    """Test that alias term normalizes to correct drug concept as an
    ALIAS match.
    """
    normalizer_response = wikidata.normalize('Mydriasine')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status

    normalizer_response = wikidata.normalize('(±)-Hyoscyamine')
    assert normalizer_response['match_type'] ==\
           MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == atropine.label
    assert normalized_drug.concept_id == atropine.concept_id
    assert set(normalized_drug.aliases) == set(atropine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(atropine.other_identifiers)
    assert set(normalized_drug.trade_names) == set(atropine.trade_names)
    assert set(normalized_drug.xrefs) == set(atropine.xrefs)
    assert normalized_drug.approval_status == atropine.approval_status


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
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == interferon_alfacon_1.label
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = wikidata.normalize('Q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = wikidata.normalize('q15353101')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status


def test_primary_label_interferon_alfacon_1(interferon_alfacon_1, wikidata):
    """Test that Interferon alfacon-1 drug normalizes to correct drug
    concept as a LABEL match.
    """
    normalizer_response = wikidata.normalize('Interferon alfacon-1')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status

    normalizer_response = wikidata.normalize('Interferon Alfacon-1')
    assert normalizer_response['match_type'] ==\
           MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.concept_id == interferon_alfacon_1.concept_id
    assert set(normalized_drug.aliases) == set(interferon_alfacon_1.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(interferon_alfacon_1.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(interferon_alfacon_1.trade_names)
    assert set(normalized_drug.xrefs) == set(interferon_alfacon_1.xrefs)
    assert normalized_drug.approval_status == \
           interferon_alfacon_1.approval_status


def test_concept_id_d_methamphetamine(d_methamphetamine, wikidata):
    """Test that d_methamphetamine drug normalizes to correct drug concept
    as a CONCEPT_ID match.
    """
    normalizer_response = wikidata.normalize('wikidata:Q191924')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == d_methamphetamine.label
    assert normalized_drug.concept_id == d_methamphetamine.concept_id
    assert set(normalized_drug.aliases) == set(d_methamphetamine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(d_methamphetamine.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(d_methamphetamine.trade_names)
    assert set(normalized_drug.xrefs) == set(d_methamphetamine.xrefs)
    assert normalized_drug.approval_status == d_methamphetamine.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:Q191924')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == d_methamphetamine.label
    assert normalized_drug.concept_id == d_methamphetamine.concept_id
    assert set(normalized_drug.aliases) == set(d_methamphetamine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(d_methamphetamine.other_identifiers)
    assert set(normalized_drug.trade_names) ==\
           set(d_methamphetamine.trade_names)
    assert set(normalized_drug.xrefs) == set(d_methamphetamine.xrefs)
    assert normalized_drug.approval_status == d_methamphetamine.approval_status

    normalizer_response = wikidata.normalize('wiKIdata:q191924')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == d_methamphetamine.label
    assert normalized_drug.concept_id == d_methamphetamine.concept_id
    assert set(normalized_drug.aliases) == set(d_methamphetamine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(d_methamphetamine.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(d_methamphetamine.trade_names)
    assert set(normalized_drug.xrefs) == set(d_methamphetamine.xrefs)
    assert normalized_drug.approval_status == d_methamphetamine.approval_status

    normalizer_response = wikidata.normalize('Q191924')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == d_methamphetamine.label
    assert normalized_drug.concept_id == d_methamphetamine.concept_id
    assert set(normalized_drug.aliases) == set(d_methamphetamine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(d_methamphetamine.other_identifiers)
    assert set(normalized_drug.trade_names) == \
           set(d_methamphetamine.trade_names)
    assert set(normalized_drug.xrefs) == set(d_methamphetamine.xrefs)
    assert normalized_drug.approval_status == d_methamphetamine.approval_status


def test_primary_label_d_methamphetamine(d_methamphetamine, wikidata):
    """Test that d_methamphetamine drug normalizes to correct drug concept
    as a LABEL match.
    """
    normalizer_response = wikidata.normalize('D-methamphetamine')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == d_methamphetamine.label
    assert normalized_drug.concept_id == d_methamphetamine.concept_id
    assert set(normalized_drug.aliases) == set(d_methamphetamine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(d_methamphetamine.other_identifiers)
    assert set(normalized_drug.trade_names) ==\
           set(d_methamphetamine.trade_names)
    assert set(normalized_drug.xrefs) == set(d_methamphetamine.xrefs)
    assert normalized_drug.approval_status == d_methamphetamine.approval_status

    normalizer_response = wikidata.normalize('d-methamphetamine')
    assert normalizer_response['match_type'] == \
           MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == d_methamphetamine.label
    assert normalized_drug.concept_id == d_methamphetamine.concept_id
    assert set(normalized_drug.aliases) == set(d_methamphetamine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(d_methamphetamine.other_identifiers)
    assert set(normalized_drug.trade_names) ==\
           set(d_methamphetamine.trade_names)
    assert set(normalized_drug.xrefs) == set(d_methamphetamine.xrefs)
    assert normalized_drug.approval_status == d_methamphetamine.approval_status


def test_meta_info(cisplatin, wikidata):
    """Test that the meta field is correct."""
    normalizer_response = wikidata.normalize('cisplatin')
    assert normalizer_response['meta_'].data_license == 'CC0 1.0'
    assert normalizer_response['meta_'].data_license_url == \
           'https://creativecommons.org/publicdomain/zero/1.0/'
    assert normalizer_response['meta_'].version == '20200812'
    assert normalizer_response['meta_'].data_url is None
