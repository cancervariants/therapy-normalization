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
        'approval_status': 'approved',
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
            'Diaminedichloroplatinum',
            'cis-Platinum',
            'CISplatin',
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
        'approval_status': 'approved',
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
            'Cisplatin',
            'Platinol'
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
            'Amiloride',
            'aMILoride',
            'Amipramidin',
            'Amipramidine',
            'N-amidino-3,5-diamino-6-chloropyrazinecarboxamide',
            'Amyloride'
        ],
        'approval_status': 'approved',
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
            'Midamor',
            'Frusemek',
            'Moduret',
            'Aridil',
            'Zida-Co',
            'Amilamont',
            'Moduretic'
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
            'Timolol',
            '(S)-1-(tert-butylamino)-3-[(4-morpholin-4-yl-1,2,5-thiadiazol'
            '-3-yl)oxy]propan-2-ol'
        ],
        'approval_status': 'approved',
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
            'Timoptic',
            'Istalol',
            'Combigan',
            'Cosopt',
            'Timoptol LA',
            'Blocadren',
            'Timolide',
            'Betim',
            'Glau-Opt',
            'Glaucol',
            'Timoptol'
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
            'Antithymocyte immunoglobulin',
            'Lymphocyte Immune Globulin, Anti-Thymocyte Globulin',
            'Anti-Thymocyte Globulin',
            'Antithymocyte Globulin',
            'Antithymoglobulin',
            'Lymphocyte Immune Globulin, Anti Thymocyte Globulin',
            'Globulins, Antithymocyte',
            'Antithymocyte Globulins',
            'Antithymoglobulins',
            'Anti-Thymocyte Globulins',
            'Globulins, Anti-Thymocyte',
            'Globulin, Antithymocyte',
            'Anti Thymocyte Globulin',
            'Globulin, Anti-Thymocyte',
            'lymphocyte immune globulin, anti-thy (obs)'
        ],
        'approval_status': 'approved',
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
            'ATGAM',
            'Thymoglobulin'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def aspirin():
    """Create aspirin drug fixture."""
    params = {
        'label': 'aspirin',
        'concept_id': 'rxcui:1191',
        'aliases': [
            'Salicylic acid acetate',
            'Aspirin',
            'Acetylsalicylic acid',
            'Acid, Acetylsalicylic',
            '2-(Acetyloxy)benzoic Acid',
            '2-Acetoxybenzenecarboxylic acid',
            'ASA',
            'O-acetylsalicylic acid',
            '2-Acetoxybenzoic acid',
            'o-acetoxybenzoic acid',
            'o-carboxyphenyl acetate',
            'Acetylsalicylic Acid'
        ],
        'approval_status': 'approved',
        'other_identifiers': [
            'drugbank:DB00945'
        ],
        'xrefs': [
            'usp:m6240',
            'gsddb:181',
            'snomedct:7947003',
            'vandf:4017536',
            'mmsl:34512',
            'mthspl:R16CO5Y76E',
            'fdbmk:001587',
            'mmsl:244',
            'snomedct:387458008',
            'msh:D001241',
            'mmsl:4223',
            'mmsl:d00170',
            'atc:A01AD05',
            'atc:B01AC06',
            'atc:N02BA01',
            'snomedct:7947003'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def mesna():
    """Create mesna drug fixture."""
    params = {
        'label': 'mesna',
        'concept_id': 'rxcui:44',
        'aliases': [
            'Sodium 2-mercaptoethanesulfonate',
            'Mesnum',
            'Ethanesulfonic acid, 2-mercapto-, monosodium salt',
            'Sodium 2-Mercaptoethanesulphonate',
            '2-Mercaptoethanesulphonate, Sodium',
            'Sodium 2-Mercaptoethanesulphonate'
        ],
        'approval_status': 'approved',
        'other_identifiers': [],
        'xrefs': [
            'usp:m49500',
            'gsddb:1950',
            'snomedct:108821000',
            'snomedct:386922000',
            'msh:D015080',
            'vandf:4019477',
            'mthspl:NR7O1405Q9',
            'fdbmk:002678',
            'atc:R05CB05',
            'atc:V03AF01',
            'mmsl:41498',
            'snomedct:386922000',
            'mmsl:5057',
            'mmsl:d01411'
        ],
        'trade_names': [
            'Mesna',
            'Mesnex',
            'Mesna Nova Plus'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def beta_alanine():
    """Create beta-alanine drug fixture."""
    params = {
        'label': 'beta-alanine',
        'concept_id': 'rxcui:61',
        'aliases': [
            'Beta alanine',
            'beta Alanine',
            '3 Aminopropionic Acid',
            '3-Aminopropionic Acid'
        ],
        'approval_status': 'approved',
        'other_identifiers': [],
        'xrefs': [
            'snomedct:70587000',
            'msh:D015091',
            'vandf:4028377',
            'fdbmk:006175',
            'mthspl:11P2JDE17B'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def algestone():
    """Create algestone drug fixture."""
    params = {
        'label': 'algestone',
        'concept_id': 'rxcui:595',
        'aliases': [
            'Pregn-4-ene-3,20-dione, 16,17-dihydroxy-, (16alpha)-',
            '16 alpha,17-Dihydroxypregn-4-ene-3,20-dione',
            'Alphasone',
            'Dihydroxyprogesterone'
        ],
        'approval_status': None,
        'other_identifiers': [],
        'xrefs': [
            'msh:D000523',
            'fdbmk:006132'
        ],
        'trade_names': []
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def levothyroxine():
    """Create levothyroxine drug fixture."""
    params = {
        'label': 'levothyroxine',
        'concept_id': 'rxcui:10582',
        'aliases': [],
        'approval_status': 'approved',
        'other_identifiers': [
            'drugbank:DB00451'
        ],
        'xrefs': [
            'snomedct:768532006',
            'gsddb:4423',
            'snomedct:73187006',
            'vandf:4022126',
            'msh:D013974',
            'mmsl:d00278',
            'mthspl:Q51BO43MG4',
            'fdbmk:005896',
            'snomedct:710809001',
            'fdbmk:004881',
            'snomedct:38076006'
        ],
        'trade_names': [
            'Eltroxin',
            'Estre',
            'Euthyrox',
            'Leventa',
            'Levo-T',
            'Levocrine',
            'Levotabs',
            'Levothroid',
            'Levothyroid',
            'Levoxyl',
            'Novothyrox',
            'SOLOXINE',
            'Synthroid',
            'Thyro-Tabs',
            'ThyroKare',
            'ThyroMed',
            'Thyrolar',
            'Thyrox',
            'Tirosint',
            'Unithroid'
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

    # Trade Name Match
    normalizer_response = rxnorm.normalize('ALIGN')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
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

    normalizer_response = rxnorm.normalize('evivo')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
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
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('CISPLATIN')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('Cis-DDP')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = rxnorm.normalize('Diaminedichloroplatinum')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    normalizer_response = rxnorm.normalize('cis Platinum')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('platinol')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == cisplatin.label
    assert normalized_drug.concept_id == cisplatin.concept_id
    assert set(normalized_drug.aliases) == set(cisplatin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(cisplatin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(cisplatin.xrefs)
    assert set(normalized_drug.trade_names) == set(cisplatin.trade_names)
    assert normalized_drug.approval_status == cisplatin.approval_status


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
    assert set(normalized_drug.aliases) == set(amiloride.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(amiloride.other_identifiers)
    assert set(normalized_drug.xrefs) == set(amiloride.xrefs)
    assert set(normalized_drug.trade_names) == set(amiloride.trade_names)
    assert normalized_drug.approval_status == amiloride.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('amiloride')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == amiloride.label
    assert normalized_drug.concept_id == amiloride.concept_id
    assert set(normalized_drug.aliases) == set(amiloride.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(amiloride.other_identifiers)
    assert set(normalized_drug.xrefs) == set(amiloride.xrefs)
    assert set(normalized_drug.trade_names) == set(amiloride.trade_names)
    assert normalized_drug.approval_status == amiloride.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('Amyloride')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == amiloride.label
    assert normalized_drug.concept_id == amiloride.concept_id
    assert set(normalized_drug.aliases) == set(amiloride.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(amiloride.other_identifiers)
    assert set(normalized_drug.xrefs) == set(amiloride.xrefs)
    assert set(normalized_drug.trade_names) == set(amiloride.trade_names)
    assert normalized_drug.approval_status == amiloride.approval_status

    normalizer_response = rxnorm.normalize('3,5-diamino-N-carbamimidoyl-6-'
                                           'chloropyrazine-2-carboxamide')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == amiloride.label
    assert normalized_drug.concept_id == amiloride.concept_id
    assert set(normalized_drug.aliases) == set(amiloride.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(amiloride.other_identifiers)
    assert set(normalized_drug.xrefs) == set(amiloride.xrefs)
    assert set(normalized_drug.trade_names) == set(amiloride.trade_names)
    assert normalized_drug.approval_status == amiloride.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('Midamor')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == amiloride.label
    assert normalized_drug.concept_id == amiloride.concept_id
    assert set(normalized_drug.aliases) == set(amiloride.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(amiloride.other_identifiers)
    assert set(normalized_drug.xrefs) == set(amiloride.xrefs)
    assert set(normalized_drug.trade_names) == set(amiloride.trade_names)
    assert normalized_drug.approval_status == amiloride.approval_status


def test_timolol(timolol, rxnorm):
    """Test that timolol drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxcUI:10600')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == timolol.label
    assert normalized_drug.concept_id == timolol.concept_id
    assert set(normalized_drug.aliases) == set(timolol.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(timolol.other_identifiers)
    assert set(normalized_drug.xrefs) == set(timolol.xrefs)
    assert set(normalized_drug.trade_names) == set(timolol.trade_names)
    assert normalized_drug.approval_status == timolol.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('timolol')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == timolol.label
    assert normalized_drug.concept_id == timolol.concept_id
    assert set(normalized_drug.aliases) == set(timolol.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(timolol.other_identifiers)
    assert set(normalized_drug.xrefs) == set(timolol.xrefs)
    assert set(normalized_drug.trade_names) == set(timolol.trade_names)
    assert normalized_drug.approval_status == timolol.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('(S)-1-(tert-butylamino)-3-[(4-'
                                           'morpholin-4-yl-1,2,5-thiadiazol'
                                           '-3-yl)oxy]propan-2-ol')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == timolol.label
    assert normalized_drug.concept_id == timolol.concept_id
    assert set(normalized_drug.aliases) == set(timolol.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(timolol.other_identifiers)
    assert set(normalized_drug.xrefs) == set(timolol.xrefs)
    assert set(normalized_drug.trade_names) == set(timolol.trade_names)
    assert normalized_drug.approval_status == timolol.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('Betimol')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == timolol.label
    assert normalized_drug.concept_id == timolol.concept_id
    assert set(normalized_drug.aliases) == set(timolol.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(timolol.other_identifiers)
    assert set(normalized_drug.xrefs) == set(timolol.xrefs)
    assert set(normalized_drug.trade_names) == set(timolol.trade_names)
    assert normalized_drug.approval_status == timolol.approval_status


def test_lymphocyte(lymphocyte, rxnorm):
    """Test that lymphocyte drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:1011')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lymphocyte.label
    assert normalized_drug.concept_id == lymphocyte.concept_id
    assert set(normalized_drug.aliases) == set(lymphocyte.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(lymphocyte.other_identifiers)
    assert set(normalized_drug.xrefs) == set(lymphocyte.xrefs)
    assert set(normalized_drug.trade_names) == set(lymphocyte.trade_names)
    assert normalized_drug.approval_status == lymphocyte.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('lymphocyte immune globulin, '
                                           'anti-thymocyte globulin')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lymphocyte.label
    assert normalized_drug.concept_id == lymphocyte.concept_id
    assert set(normalized_drug.aliases) == set(lymphocyte.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(lymphocyte.other_identifiers)
    assert set(normalized_drug.xrefs) == set(lymphocyte.xrefs)
    assert set(normalized_drug.trade_names) == set(lymphocyte.trade_names)
    assert normalized_drug.approval_status == lymphocyte.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('Anti Thymocyte Globulin')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lymphocyte.label
    assert normalized_drug.concept_id == lymphocyte.concept_id
    assert set(normalized_drug.aliases) == set(lymphocyte.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(lymphocyte.other_identifiers)
    assert set(normalized_drug.xrefs) == set(lymphocyte.xrefs)
    assert set(normalized_drug.trade_names) == set(lymphocyte.trade_names)
    assert normalized_drug.approval_status == lymphocyte.approval_status

    normalizer_response = rxnorm.normalize('Antithymoglobulin')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lymphocyte.label
    assert normalized_drug.concept_id == lymphocyte.concept_id
    assert set(normalized_drug.aliases) == set(lymphocyte.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(lymphocyte.other_identifiers)
    assert set(normalized_drug.xrefs) == set(lymphocyte.xrefs)
    assert set(normalized_drug.trade_names) == set(lymphocyte.trade_names)
    assert normalized_drug.approval_status == lymphocyte.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('Thymoglobulin')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lymphocyte.label
    assert normalized_drug.concept_id == lymphocyte.concept_id
    assert set(normalized_drug.aliases) == set(lymphocyte.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(lymphocyte.other_identifiers)
    assert set(normalized_drug.xrefs) == set(lymphocyte.xrefs)
    assert set(normalized_drug.trade_names) == set(lymphocyte.trade_names)
    assert normalized_drug.approval_status == lymphocyte.approval_status

    normalizer_response = rxnorm.normalize('ATGAM')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == lymphocyte.label
    assert normalized_drug.concept_id == lymphocyte.concept_id
    assert set(normalized_drug.aliases) == set(lymphocyte.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(lymphocyte.other_identifiers)
    assert set(normalized_drug.xrefs) == set(lymphocyte.xrefs)
    assert set(normalized_drug.trade_names) == set(lymphocyte.trade_names)
    assert normalized_drug.approval_status == lymphocyte.approval_status


def test_aspirin(aspirin, rxnorm):
    """Test that aspirin drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxcUI:1191')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aspirin.label
    assert normalized_drug.concept_id == aspirin.concept_id
    assert set(normalized_drug.aliases) == set(aspirin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aspirin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aspirin.xrefs)
    assert set(normalized_drug.trade_names) == set(aspirin.trade_names)
    assert normalized_drug.approval_status == aspirin.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('aspirin')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aspirin.label
    assert normalized_drug.concept_id == aspirin.concept_id
    assert set(normalized_drug.aliases) == set(aspirin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aspirin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aspirin.xrefs)
    assert set(normalized_drug.trade_names) == set(aspirin.trade_names)
    assert normalized_drug.approval_status == aspirin.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('Acetylsalicylic Acid')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aspirin.label
    assert normalized_drug.concept_id == aspirin.concept_id
    assert set(normalized_drug.aliases) == set(aspirin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aspirin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aspirin.xrefs)
    assert set(normalized_drug.trade_names) == set(aspirin.trade_names)
    assert normalized_drug.approval_status == aspirin.approval_status

    normalizer_response = rxnorm.normalize('ASA')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == aspirin.label
    assert normalized_drug.concept_id == aspirin.concept_id
    assert set(normalized_drug.aliases) == set(aspirin.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(aspirin.other_identifiers)
    assert set(normalized_drug.xrefs) == set(aspirin.xrefs)
    assert set(normalized_drug.trade_names) == set(aspirin.trade_names)
    assert normalized_drug.approval_status == aspirin.approval_status

    # (Trade Name) No Match
    normalizer_response = rxnorm.normalize('Anacin')
    assert normalizer_response['match_type'] == MatchType.NO_MATCH
    assert len(normalizer_response['records']) == 0


def test_mesnan(mesna, rxnorm):
    """Test that mesna drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:44')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == mesna.label
    assert normalized_drug.concept_id == mesna.concept_id
    assert set(normalized_drug.aliases) == set(mesna.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(mesna.other_identifiers)
    assert set(normalized_drug.xrefs) == set(mesna.xrefs)
    assert set(normalized_drug.trade_names) == set(mesna.trade_names)
    assert normalized_drug.approval_status == mesna.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('mesna')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == mesna.label
    assert normalized_drug.concept_id == mesna.concept_id
    assert set(normalized_drug.aliases) == set(mesna.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(mesna.other_identifiers)
    assert set(normalized_drug.xrefs) == set(mesna.xrefs)
    assert set(normalized_drug.trade_names) == set(mesna.trade_names)
    assert normalized_drug.approval_status == mesna.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('Mesnum')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == mesna.label
    assert normalized_drug.concept_id == mesna.concept_id
    assert set(normalized_drug.aliases) == set(mesna.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(mesna.other_identifiers)
    assert set(normalized_drug.xrefs) == set(mesna.xrefs)
    assert set(normalized_drug.trade_names) == set(mesna.trade_names)
    assert normalized_drug.approval_status == mesna.approval_status

    normalizer_response = rxnorm.normalize('Ethanesulfonic acid, 2-mercapto-, '
                                           'monosodium salt')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == mesna.label
    assert normalized_drug.concept_id == mesna.concept_id
    assert set(normalized_drug.aliases) == set(mesna.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(mesna.other_identifiers)
    assert set(normalized_drug.xrefs) == set(mesna.xrefs)
    assert set(normalized_drug.trade_names) == set(mesna.trade_names)
    assert normalized_drug.approval_status == mesna.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('Mesnex')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == mesna.label
    assert normalized_drug.concept_id == mesna.concept_id
    assert set(normalized_drug.aliases) == set(mesna.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(mesna.other_identifiers)
    assert set(normalized_drug.xrefs) == set(mesna.xrefs)
    assert set(normalized_drug.trade_names) == set(mesna.trade_names)
    assert normalized_drug.approval_status == mesna.approval_status


def test_beta_alanine(beta_alanine, rxnorm):
    """Test that beta_alanine drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:61')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == beta_alanine.label
    assert normalized_drug.concept_id == beta_alanine.concept_id
    assert set(normalized_drug.aliases) == set(beta_alanine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(beta_alanine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(beta_alanine.xrefs)
    assert set(normalized_drug.trade_names) == set(beta_alanine.trade_names)
    assert normalized_drug.approval_status == beta_alanine.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('beta-alanine')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == beta_alanine.label
    assert normalized_drug.concept_id == beta_alanine.concept_id
    assert set(normalized_drug.aliases) == set(beta_alanine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(beta_alanine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(beta_alanine.xrefs)
    assert set(normalized_drug.trade_names) == set(beta_alanine.trade_names)
    assert normalized_drug.approval_status == beta_alanine.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('3 Aminopropionic Acid')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == beta_alanine.label
    assert normalized_drug.concept_id == beta_alanine.concept_id
    assert set(normalized_drug.aliases) == set(beta_alanine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(beta_alanine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(beta_alanine.xrefs)
    assert set(normalized_drug.trade_names) == set(beta_alanine.trade_names)
    assert normalized_drug.approval_status == beta_alanine.approval_status


def test_algestone(algestone, rxnorm):
    """Test that algestone drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:595')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == algestone.label
    assert normalized_drug.concept_id == algestone.concept_id
    assert set(normalized_drug.aliases) == set(algestone.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(algestone.other_identifiers)
    assert set(normalized_drug.xrefs) == set(algestone.xrefs)
    assert set(normalized_drug.trade_names) == set(algestone.trade_names)
    assert normalized_drug.approval_status == algestone.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('algestone')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == algestone.label
    assert normalized_drug.concept_id == algestone.concept_id
    assert set(normalized_drug.aliases) == set(algestone.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(algestone.other_identifiers)
    assert set(normalized_drug.xrefs) == set(algestone.xrefs)
    assert set(normalized_drug.trade_names) == set(algestone.trade_names)
    assert normalized_drug.approval_status == algestone.approval_status

    # Alias Match
    normalizer_response = rxnorm.normalize('Pregn-4-ene-3,20-dione, 16,'
                                           '17-dihydroxy-, (16alpha)-')
    assert normalizer_response['match_type'] == MatchType.ALIAS
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == algestone.label
    assert normalized_drug.concept_id == algestone.concept_id
    assert set(normalized_drug.aliases) == set(algestone.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(algestone.other_identifiers)
    assert set(normalized_drug.xrefs) == set(algestone.xrefs)
    assert set(normalized_drug.trade_names) == set(algestone.trade_names)
    assert normalized_drug.approval_status == algestone.approval_status


def test_levothyroxine(levothyroxine, rxnorm):
    """Test that levothyroxine drug normalizes to correct drug concept."""
    # Concept ID Match
    normalizer_response = rxnorm.normalize('RxCUI:10582')
    assert normalizer_response['match_type'] == MatchType.CONCEPT_ID
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == levothyroxine.label
    assert normalized_drug.concept_id == levothyroxine.concept_id
    assert set(normalized_drug.aliases) == set(levothyroxine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(levothyroxine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(levothyroxine.xrefs)
    assert set(normalized_drug.trade_names) == set(levothyroxine.trade_names)
    assert normalized_drug.approval_status == levothyroxine.approval_status

    # Label Match
    normalizer_response = rxnorm.normalize('levothyroxine')
    assert normalizer_response['match_type'] == MatchType.LABEL
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == levothyroxine.label
    assert normalized_drug.concept_id == levothyroxine.concept_id
    assert set(normalized_drug.aliases) == set(levothyroxine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(levothyroxine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(levothyroxine.xrefs)
    assert set(normalized_drug.trade_names) == set(levothyroxine.trade_names)
    assert normalized_drug.approval_status == levothyroxine.approval_status

    # Trade Name Match
    normalizer_response = rxnorm.normalize('Unithroid')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == levothyroxine.label
    assert normalized_drug.concept_id == levothyroxine.concept_id
    assert set(normalized_drug.aliases) == set(levothyroxine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(levothyroxine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(levothyroxine.xrefs)
    assert set(normalized_drug.trade_names) == set(levothyroxine.trade_names)
    assert normalized_drug.approval_status == levothyroxine.approval_status

    normalizer_response = rxnorm.normalize('Euthyrox')
    assert normalizer_response['match_type'] == MatchType.TRADE_NAME
    assert len(normalizer_response['records']) == 1
    normalized_drug = normalizer_response['records'][0]
    assert normalized_drug.label == levothyroxine.label
    assert normalized_drug.concept_id == levothyroxine.concept_id
    assert set(normalized_drug.aliases) == set(levothyroxine.aliases)
    assert set(normalized_drug.other_identifiers) == \
           set(levothyroxine.other_identifiers)
    assert set(normalized_drug.xrefs) == set(levothyroxine.xrefs)
    assert set(normalized_drug.trade_names) == set(levothyroxine.trade_names)
    assert normalized_drug.approval_status == levothyroxine.approval_status


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
