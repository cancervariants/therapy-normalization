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
def bifidobacterium_infantis():
    """Create bifidobacterium infantis drug fixture."""
    params = {
        'label': 'Bifidobacterium Infantis',
        'concept_id': 'rxcui:100213',
        'aliases': [
            'bifidobacterium infantis',
            'Bifidobacterium infantis'
        ],
        'approval_status': None,
        'other_identifiers': [
            'drugbank:DB14222'
        ],
        'xrefs': [
            'gsddb:4931',
            'mmsl:d07347',
            'fdbmk:001165'
        ],
        'trade_names': [
            'Align',
            'Evivo'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def cisplatin():
    """Create cisplatin drug fixture."""
    params = {
        'label': 'cisplatin',
        'concept_id': 'rxcui:2555',
        'aliases': [
            'Cisplatin-containing product',
            'Diaminedichloroplatinum',
            'cis-Platinum',
            'CISplatin',
            'Cisplatin',
            'cis Diamminedichloroplatinum',
            'cis Platinum',
            'Diamminodichloride, Platinum',
            'cis-Platinum compound',
            'cis-DDP',
            'cis-Diaminedichloroplatinum',
            'cis-Platinum II',
            'DDP',
            'Dichlorodiammineplatinum',
            'cis-Diamminedichloroplatinum',
            'cis-Dichlorodiammineplatinum(II)',
            'Platinum Diamminodichloride',
            'cis-Diamminedichloroplatinum(II)',
            'cis-Platinum',
            'cis-diamminedichloroplatinum(II)',
            'CDDP',
            'Cis-DDP',
        ],
        'approval_status': None,
        'other_identifiers': [
            'drugbank:DB00515',
            'drugbank:DB12117'
        ],
        'xrefs': [
            'usp:m17910',
            'gsddb:862',
            'snomedct:57066004',
            'vandf:4018139',
            'msh:D002945',
            'mthspl:Q20Q21Q62J',
            'mmsl:d00195',
            'fdbmk:002641',
            'atc:L01XA01',
            'mmsl:31747',
            'mmsl:4456',
            'snomedct:387318005'
        ],
        'trade_names': [
            'Cisplatin'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def amiloride():
    """Create amiloride hydrochloride, anhydrous drug fixture."""
    params = {
        'label': 'amiloride',
        'concept_id': 'rxcui:644',
        'aliases': [
            '3,5-diamino-N-carbamimidoyl-6-chloropyrazine-2-carboxamide',
            'Amiloride-containing product',
            'Amiloride',
            'aMILoride',
            'Amipramidin',
            'Amipramidine',
            'N-amidino-3,5-diamino-6-chloropyrazinecarboxamide',
            'Amyloride'
        ],
        'approval_status': None,
        'other_identifiers': [
            'drugbank:DB00594'
        ],
        'xrefs': [
            'gsddb:3993',
            'snomedct:387503008',
            'msh:D000584',
            'vandf:4019603',
            'fdbmk:004861',
            'mthspl:7DZO8EB0Z3',
            'mmsl:d00169',
            'atc:C03DB01',
            'snomedct:87395005'
        ],
        'trade_names': [
            'Midamor'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def timolol():
    """Create timolol drug fixture."""
    params = {
        'label': 'timolol',
        'concept_id': 'rxcui:10600',
        'aliases': [
            '(S)-1-((1,1-Dimethylethyl)amino)-3-((4-(4-morpholinyl)'
            '-1,2,5-thiadazol-3-yl)oxy)-2-propanol',
            '2-Propanol, 1-((1,1-dimethylethyl)amino)-3-((4-(4-morpholi'
            'nyl)-1,2,5-thiadiazol-3-yl)oxy)-, (S)-',
            'Timolol-containing product',
            'Timolol',
            'timolol',
            '(S)-1-(tert-butylamino)-3-[(4-morpholin-4-yl-1,2,5-thiadiazol'
            '-3-yl)oxy]propan-2-ol'
        ],
        'approval_status': None,
        'other_identifiers': [
            'drugbank:DB00373'
        ],
        'xrefs': [
            'vandf:4019949',
            'mthspl:817W3C6175',
            'snomedct:372880004',
            'msh:D013999',
            'mmsl:d00139',
            'atc:C07AA06',
            'atc:S01ED01',
            'fdbmk:004741',
            'snomedct:85591001'
        ],
        'trade_names': [
            'Betimol',
            'Timoptic'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def lymphocyte():
    """Create lymphocyte immune globulin, anti-thymocyte globulin drug
    fixture.
    """
    params = {
        'label': 'lymphocyte immune globulin, anti-thymocyte globulin',
        'concept_id': 'rxcui:1011',
        'aliases': [
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'snomedct:768651008',
            'msh:D000961',
            'vandf:4022194',
            'vandf:4018097',
            'mmsl:d01141',
            'mmsl:5001'
        ],
        'trade_names': [
        ]
    }
    return Drug(**params)


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
    normalizer_response = rxnorm.normalize(' Bifidobacterium infantis')
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

    normalizer_response = rxnorm.normalize('bifidobacterium infantis')
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


def test_cisplatin(cisplatin, rxnorm):
    """Test that cisplatin drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:2555')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == \
           set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(cisplatin.trade_names)
    assert normalized_drug.approval_status == \
           cisplatin.approval_status


def test_amiloride(amiloride, rxnorm):
    """Test that amiloride hydrochloride, anhydrous drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxcUI:644')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == amiloride.label
    assert normalized_drug.concept_id == amiloride.concept_id
    assert set(normalized_drug.aliases) == \
           set(amiloride.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(amiloride.other_identifiers)
    assert set(normalized_drug.xrefs) == set(amiloride.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(amiloride.trade_names)
    assert normalized_drug.approval_status == \
           amiloride.approval_status


def test_timolol(timolol, rxnorm):
    """Test that timolol drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxcUI:10600')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == timolol.label
    assert normalized_drug.concept_id == timolol.concept_id
    assert set(normalized_drug.aliases) == \
           set(timolol.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(timolol.other_identifiers)
    assert set(normalized_drug.xrefs) == set(timolol.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(timolol.trade_names)
    assert normalized_drug.approval_status == \
           timolol.approval_status


def test_no_match(rxnorm):
    """Test that a term normalizes to correct drug concept as a NO match."""
    # Misspelled name
    normalizer_response = rxnorm.normalize('17-hydroxycorticosteroi')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Not storing foreign synonyms
    normalizer_response = rxnorm.normalize('cisplatino')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0

    # Wrong Namespace
    normalizer_response = rxnorm.normalize('rxnorm:3')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test white space in between id
    normalizer_response = rxnorm.normalize('rxcui: 3')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Should not store brand name concepts as identity record
    normalizer_response = rxnorm.normalize('rxcui:202433')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    normalizer_response = rxnorm.normalize('')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_meta_info(rxnorm):
    """Test that the meta field is correct."""
    normalizer_response = rxnorm.fetch_meta()
    assert normalizer_response.data_license == 'UMLS Metathesaurus'
    assert normalizer_response.data_license_url == \
           'https://www.nlm.nih.gov/research/umls/rxnorm/docs/' \
           'termsofservice.html'
    assert normalizer_response.version == '20210104'
    assert normalizer_response.data_url == \
           'https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html'
    assert not normalizer_response.rdp_url
    assert normalizer_response.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": False
    }
