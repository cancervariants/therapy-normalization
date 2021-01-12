"""Test that the therapy normalizer works as intended for the RxNorm source."""
import pytest
from therapy.schemas import Drug, MatchType
from therapy.query import Normalizer


@pytest.fixture(scope='module')
def rxnorm():
    """Build RxNorm normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = Normalizer()

        def normalize(self, query_str):
            resp = self.normalizer.normalize(query_str, keyed=True,
                                             incl='rxnorm')
            return resp['source_matches']['RxNorm']

        def fetch_meta(self):
            return self.normalizer.fetch_meta('RxNorm')

    return QueryGetter()


@pytest.fixture(scope='module')
def hydrocorticosteroid():
    """Create a 17-hydrocorticosteroid drug fixture."""
    params = {
        'label': '17-hydroxycorticosteroid',
        'concept_id': 'rxcui:19',
        'aliases': [
            '17-hydrocorticosteroid',
            '17-hydroxycorticoid',
            '17-hydroxycorticosteroid (substance)'
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'snomedct:112116001'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def aquanil_lotion():
    """Create AQUANIL LOTION drug fixture."""
    params = {
        'label': 'AQUANIL LOTION',
        'concept_id': 'rxcui:91837',
        'aliases': [],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'mmsl:5035',
            'vandf:4010585',
            'mmx:106710'
        ],
        'trade_names': [
            'Aquanil Lotion, topical lotion',
            'Lotion, Multi Ingredient Topical application Lotion [AQUANIL]'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def bifidobacterium_infantis():
    """Create bifidobacterium infantis drug fixture."""
    params = {
        'label': 'bifidobacterium infantis',
        'concept_id': 'rxcui:100213',
        'aliases': [
            'Bifidobacterium infantis'
        ],
        'approval_status': None,
        'other_identifiers': [
            'drugbank:db14222'
        ],
        'xrefs': [
            'gsddb:4931',
            'mmsl:d07347',
            'fdbmk:001165'
        ],
        'trade_names': []
    }
    return Drug(**params)


def test_hydrocorticosteroid(hydrocorticosteroid, rxnorm):
    """Test that hydrocorticosteroid drug normalizes to correct drug
    concept.
    """
    # Concept ID Match
    normalizer_response = rxnorm.normalize('rxcui:19')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == hydrocorticosteroid.label
    assert normalized_drug.concept_id == hydrocorticosteroid.concept_id
    assert set(normalized_drug.aliases) == set(hydrocorticosteroid.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(hydrocorticosteroid.other_identifiers)
    assert set(normalized_drug.xrefs) == set(hydrocorticosteroid.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(hydrocorticosteroid.trade_names)
    assert normalized_drug.approval_status \
           == hydrocorticosteroid.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('17-hydroxycorticosteroid')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == hydrocorticosteroid.label
    assert normalized_drug.concept_id == hydrocorticosteroid.concept_id
    assert set(normalized_drug.aliases) == set(hydrocorticosteroid.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(hydrocorticosteroid.other_identifiers)
    assert set(normalized_drug.xrefs) == set(hydrocorticosteroid.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(hydrocorticosteroid.trade_names)
    assert normalized_drug.approval_status \
           == hydrocorticosteroid.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('17-hydroxycorticoid')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == hydrocorticosteroid.label
    assert normalized_drug.concept_id == hydrocorticosteroid.concept_id
    assert set(normalized_drug.aliases) == set(hydrocorticosteroid.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(hydrocorticosteroid.other_identifiers)
    assert set(normalized_drug.xrefs) == set(hydrocorticosteroid.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(hydrocorticosteroid.trade_names)
    assert normalized_drug.approval_status \
           == hydrocorticosteroid.approval_status

    normalizer_response = \
        rxnorm.normalize('17-hydroxycorticosteroid (substance)')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == hydrocorticosteroid.label
    assert normalized_drug.concept_id == hydrocorticosteroid.concept_id
    assert set(normalized_drug.aliases) == set(hydrocorticosteroid.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(hydrocorticosteroid.other_identifiers)
    assert set(normalized_drug.xrefs) == set(hydrocorticosteroid.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(hydrocorticosteroid.trade_names)
    assert normalized_drug.approval_status \
           == hydrocorticosteroid.approval_status


def test_aquanil_lotion(aquanil_lotion, rxnorm):
    """Test that aquanil_lotion drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RXCUI:91837')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aquanil_lotion.label
    assert normalized_drug.concept_id == aquanil_lotion.concept_id
    assert set(normalized_drug.aliases) == set(aquanil_lotion.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aquanil_lotion.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aquanil_lotion.xrefs)
    assert set(normalized_drug.trade_names) == set(aquanil_lotion.trade_names)
    assert normalized_drug.approval_status == aquanil_lotion.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('AQUANIL lotion  ')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aquanil_lotion.label
    assert normalized_drug.concept_id == aquanil_lotion.concept_id
    assert set(normalized_drug.aliases) == set(aquanil_lotion.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aquanil_lotion.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aquanil_lotion.xrefs)
    assert set(normalized_drug.trade_names) == set(aquanil_lotion.trade_names)
    assert normalized_drug.approval_status == aquanil_lotion.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('Aquanil Lotion, topical lotion')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aquanil_lotion.label
    assert normalized_drug.concept_id == aquanil_lotion.concept_id
    assert set(normalized_drug.aliases) == set(aquanil_lotion.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aquanil_lotion.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aquanil_lotion.xrefs)
    assert set(normalized_drug.trade_names) == set(aquanil_lotion.trade_names)
    assert normalized_drug.approval_status == aquanil_lotion.approval_status

    normalizer_response = rxnorm.normalize('Lotion, Multi Ingredient Topical '
                                           'application Lotion [AQUANIL]')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aquanil_lotion.label
    assert normalized_drug.concept_id == aquanil_lotion.concept_id
    assert set(normalized_drug.aliases) == set(aquanil_lotion.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aquanil_lotion.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aquanil_lotion.xrefs)
    assert set(normalized_drug.trade_names) == set(aquanil_lotion.trade_names)
    assert normalized_drug.approval_status == aquanil_lotion.approval_status


def test_bifidobacterium_infantis(bifidobacterium_infantis, rxnorm):
    """Test that bifidobacterium_ nfantis drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:100213')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == bifidobacterium_infantis.label
    assert normalized_drug.concept_id == bifidobacterium_infantis.concept_id
    assert set(normalized_drug.aliases) == \
           set(bifidobacterium_infantis.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(bifidobacterium_infantis.other_identifiers)
    assert set(normalized_drug.xrefs) == set(bifidobacterium_infantis.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(bifidobacterium_infantis.trade_names)
    assert normalized_drug.approval_status == \
           bifidobacterium_infantis.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize(' bifidobacterium infantis')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == bifidobacterium_infantis.label
    assert normalized_drug.concept_id == bifidobacterium_infantis.concept_id
    assert set(normalized_drug.aliases) == \
           set(bifidobacterium_infantis.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(bifidobacterium_infantis.other_identifiers)
    assert set(normalized_drug.xrefs) == set(bifidobacterium_infantis.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(bifidobacterium_infantis.trade_names)
    assert normalized_drug.approval_status == \
           bifidobacterium_infantis.approval_status

    normalizer_response = rxnorm.normalize('Bifidobacterium infantis')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == bifidobacterium_infantis.label
    assert normalized_drug.concept_id == bifidobacterium_infantis.concept_id
    assert set(normalized_drug.aliases) == \
           set(bifidobacterium_infantis.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(bifidobacterium_infantis.other_identifiers)
    assert set(normalized_drug.xrefs) == set(bifidobacterium_infantis.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(bifidobacterium_infantis.trade_names)
    assert normalized_drug.approval_status == \
           bifidobacterium_infantis.approval_status


def test_no_match(rxnorm):
    """Test that a term normalizes to correct drug concept as a NO match."""
    normalizer_response = rxnorm.normalize('17-hydroxycorticosteroi')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Test white space in between id
    normalizer_response = rxnorm.normalize('rxcui: 3')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    normalizer_response = rxnorm.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_meta_info(rxnorm):
    """Test that the meta field is correct."""
    normalizer_response = rxnorm.fetch_meta()
    assert normalizer_response.data_license == 'NLM'
    assert normalizer_response.data_license_url == \
           'https://www.nlm.nih.gov/research/umls/rxnorm/docs/' \
           'termsofservice.html'
    assert normalizer_response.version == '01042021'
    assert normalizer_response.data_url == \
           'https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html'
    assert not normalizer_response.rdp_url
    assert normalizer_response.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True
    }
