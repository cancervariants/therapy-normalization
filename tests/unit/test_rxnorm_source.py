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
        'label': '17-hydrocorticosteroid',
        'concept_id': 'rxcui:19',
        'aliases': [
            '17-hydroxycorticoid',
            '17-hydroxycorticosteroid',
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
        'label': 'Aquanil Lotion, topical lotion',
        'concept_id': 'rxcui:91837',
        'aliases': [
            'AQUANIL LOTION'
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'mmsl:5035',
            'vandf:4010585',
            'mmx:106710'
        ],
        'trade_names': [
            'Lotion, Multi Ingredient Topical application Lotion [AQUANIL]'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def bifidobacterium_infantis():
    """Create bifidobacterium infantis drug fixture."""
    params = {
        'label': 'Bifidobacterium infantis',
        'concept_id': 'rxcui:100213',
        'aliases': [
            'bifidobacterium infantis'
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
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def cisplatin():
    """Create cisplatin drug fixture."""
    params = {
        'label': 'Cisplatin',
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
            'Cisplatin (substance)',
            'Dichlorodiammineplatinum',
            'cis-Diamminedichloroplatinum',
            'cis-Dichlorodiammineplatinum(II)',
            'Platinum Diamminodichloride',
            'cis-Diamminedichloroplatinum(II)',
            'cis-Platinum',
            'cis-diamminedichloroplatinum(II)',
            'CDDP',
            'Cis-DDP',
            'Product containing cisplatin (medicinal product)'
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
def nifedipine():
    """Create nifedipine 10 MG Oral Capsule [Adalat] drug fixture."""
    params = {
        'label': 'nifedipine 10 MG Oral Capsule [Adalat]',
        'concept_id': 'rxcui:91691',
        'aliases': [
            'NIFEdipine 10 MG Oral Capsule [Adalat]',
            'Adalat 10 MG Oral Capsule'
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'gsddb:18734',
            'mmx:102718',
            'mmsl:2735'
        ],
        'trade_names': [
            'Adalat 10mg Capsule',
            'Nifedipine 10 MG Oral Capsule, Liquid Filled [ADALAT]',
            'Adalat, 10 mg oral capsule'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def amiloride():
    """Create amiloride hydrochloride, anhydrous drug fixture."""
    params = {
        'label': 'amiloride hydrochloride, anhydrous',
        'concept_id': 'rxcui:1298837',
        'aliases': [
            'aMILoride hydrochloride, anhydrous',
            'Anhydrous Amiloride Hydrochloride',
            'Hydrochloride, Anhydrous Amiloride',
            'Amiloride Hydrochloride, Anhydrous'
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'msh:D000584',
            'mthspl:7M458Q65S3'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def aspirin():
    """Create aspirin drug fixture."""
    params = {
        'label': 'Aspirin',
        'concept_id': 'rxcui:1191',
        'aliases': [
            'Aspirin-containing product',
            'Salicylic acid acetate',
            'Aspirin',
            'aspirin',
            'Acetylsalicylic acid',
            'Acid, Acetylsalicylic',
            'Aspirin (substance)',
            '2-(Acetyloxy)benzoic Acid',
            'Acetylsalicylic Acid',
            '2-Acetoxybenzenecarboxylic acid',
            'ASA',
            'O-acetylsalicylic acid',
            '2-Acetoxybenzoic acid',
            'o-acetoxybenzoic acid',
            'o-carboxyphenyl acetate',
            'Product containing aspirin (medicinal product)'
        ],
        'approval_status': None,
        'other_identifiers': [
            'drugbank:DB00945'
        ],
        'xrefs': [
            'usp:m6240',
            'gsddb:181',
            'snomedct:7947003',
            'snomedct:387458008',
            'vandf:4017536',
            'mmsl:34512',
            'mmsl:4223',
            'mmsl:d00170',
            'mmsl:244',
            'mthspl:R16CO5Y76E',
            'fdbmk:001587',
            'msh:D001241',
            'atc:A01AD05',
            'atc:B01AC06',
            'atc:N02BA01'
        ],
        'trade_names': [
            'Acetylsalicylic Acid',
            'Aspirin'
        ]
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
    normalizer_response = rxnorm.normalize('17-hydrocorticosteroid')
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

    normalizer_response = rxnorm.normalize('17-hydroxycorticosteroid')
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
    normalizer_response = rxnorm.normalize('Aquanil Lotion, topical lotion  ')
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


def test_nifedipine(nifedipine, rxnorm):
    """Test that nifedipine drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:91691')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == nifedipine.label
    assert normalized_drug.concept_id == nifedipine.concept_id
    assert set(normalized_drug.aliases) == \
           set(nifedipine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(nifedipine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(nifedipine.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(nifedipine.trade_names)
    assert normalized_drug.approval_status == \
           nifedipine.approval_status


def test_amiloride(amiloride, rxnorm):
    """Test that amiloride hydrochloride, anhydrous drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxcUI:1298837')
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


def test_aspirin(aspirin, rxnorm):
    """Test that aspirin drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxcUI:1191')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aspirin.label
    assert normalized_drug.concept_id == aspirin.concept_id
    assert set(normalized_drug.aliases) == \
           set(aspirin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aspirin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aspirin.xrefs)
    assert set(normalized_drug.trade_names) == \
           set(aspirin.trade_names)
    assert normalized_drug.approval_status == \
           aspirin.approval_status


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

    # Not including TTY = IN (Ingredient)
    normalizer_response = rxnorm.normalize('electrolytes for oral solution')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH

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
