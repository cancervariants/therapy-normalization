"""Test that the therapy normalizer works as intended for the RxNorm source."""
import pytest
from tests.conftest import compare_records
from therapy.schemas import Drug, MatchType
from therapy.query import QueryHandler
from therapy.database import Database
import os
from boto3.dynamodb.conditions import Key


@pytest.fixture(scope='module')
def rxnorm():
    """Build RxNorm normalizer test fixture."""
    class QueryGetter:

        def __init__(self):
            self.normalizer = QueryHandler()
            if 'THERAPY_NORM_DB_URL' in os.environ:
                db_url = os.environ['THERAPY_NORM_DB_URL']
            else:
                db_url = 'http://localhost:8000'
            self.db = Database(db_url=db_url)

        def search(self, query_str):
            resp = self.normalizer.search_sources(query_str, keyed=True,
                                                  incl='rxnorm')
            return resp['source_matches']['RxNorm']

        def fetch_meta(self):
            return self.normalizer._fetch_meta('RxNorm')

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
        'xrefs': [
            'drugbank:DB14222'
        ],
        'associated_with': [
            'mmsl:d07347'
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
            'cis-Platinum',
            'CISplatin',
            'cis Diamminedichloroplatinum',
            'cis Platinum',
            'Diamminodichloride, Platinum',
            'cis-Diamminedichloroplatinum',
            'DDP',
            'Dichlorodiammineplatinum',
            'cis-Dichlorodiammineplatinum(II)',
            'Platinum Diamminodichloride',
            'cis-Diamminedichloroplatinum(II)',
            'cis-diamminedichloroplatinum(II)',
            'CDDP',
            'Cis-DDP',
        ],
        'approval_status': 'approved',
        'xrefs': [
            'drugbank:DB00515',
            'drugbank:DB12117'
        ],
        'associated_with': [
            'usp:m17910',
            'vandf:4018139',
            'mesh:D002945',
            'mthspl:Q20Q21Q62J',
            'mmsl:d00195',
            'atc:L01XA01',
            'mmsl:31747',
            'mmsl:4456',
        ],
        'trade_names': [
            'Cisplatin',
            'Platinol'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def amiloride_hydrochloride():
    """Create amiloride hydrochloride drug fixture."""
    params = {
        'label': 'amiloride hydrochloride',
        'concept_id': 'rxcui:142424',
        'aliases': [
            'aMILoride hydrochloride',
            'Amiloride Hydrochloride',
            'Hydrochloride, Amiloride'
        ],
        'approval_status': 'approved',
        'xrefs': [],
        'associated_with': [
            'usp:m2650',
            'mthspl:FZJ37245UC',
            'mmsl:2658',
            'mesh:D000584',
            'mmsl:4166',
            'vandf:4017935'
        ],
        'trade_names': [
            'AMILoride Hydrochloride',
            'Midamor',
            'Aridil',
            'Frusemek',
            'Midamor',
            'Moduret',
            'Moduretic',
            'Zida-Co'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def amiloride():
    """Create amiloride drug fixture."""
    params = {
        'label': 'amiloride',
        'concept_id': 'rxcui:644',
        'aliases': [
            '3,5-diamino-N-carbamimidoyl-6-chloropyrazine-2-carboxamide',
            'aMILoride',
            'Amipramidin',
            'Amipramidine',
            'N-amidino-3,5-diamino-6-chloropyrazinecarboxamide',
            'Amyloride'
        ],
        'approval_status': 'approved',
        'xrefs': [
            'drugbank:DB00594'
        ],
        'associated_with': [
            'mesh:D000584',
            'vandf:4019603',
            'mthspl:7DZO8EB0Z3',
            'mmsl:d00169',
            'atc:C03DB01'
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
            '(S)-1-(tert-butylamino)-3-[(4-morpholin-4-yl-1,2,5-thiadiazol'
            '-3-yl)oxy]propan-2-ol'
        ],
        'approval_status': 'approved',
        'xrefs': [
            'drugbank:DB00373'
        ],
        'associated_with': [
            'vandf:4019949',
            'mthspl:817W3C6175',
            'mesh:D013999',
            'mmsl:d00139',
            'atc:C07AA06',
            'atc:S01ED01'
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
        'xrefs': [],
        'associated_with': [
            'mesh:D000961',
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
        'xrefs': [
            'drugbank:DB00945'
        ],
        'associated_with': [
            'usp:m6240',
            'vandf:4017536',
            'mmsl:34512',
            'mthspl:R16CO5Y76E',
            'mmsl:244',
            'mesh:D001241',
            'mmsl:4223',
            'mmsl:d00170',
            'atc:A01AD05',
            'atc:B01AC06',
            'atc:N02BA01'
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
            'Mesnum',
            'Ethanesulfonic acid, 2-mercapto-, monosodium salt',
            'Sodium 2-Mercaptoethanesulphonate',
            '2-Mercaptoethanesulphonate, Sodium',
            'Sodium 2-Mercaptoethanesulphonate'
        ],
        'approval_status': 'approved',
        'xrefs': [],
        'associated_with': [
            'usp:m49500',
            'mesh:D015080',
            'vandf:4019477',
            'mthspl:NR7O1405Q9',
            'atc:R05CB05',
            'atc:V03AF01',
            'mmsl:41498',
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
            'beta Alanine',
            '3 Aminopropionic Acid',
            '3-Aminopropionic Acid'
        ],
        'approval_status': 'approved',
        'xrefs': [],
        'associated_with': [
            'mesh:D015091',
            'vandf:4028377',
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
        'xrefs': [],
        'associated_with': [
            'mesh:D000523'
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
        'aliases': [
            '3,5,3\',5\'-Tetraiodo-L-thyronine',
            'Thyroxine',
            'thyroxine',
            'Thyroid Hormone, T4',
            'Thyroxin',
            'T4 Thyroid Hormone',
            'O-(4-Hydroxy-3,5-diiodophenyl)-3,5-diiodotyrosine',
            '3,5,3\',5\'-Tetraiodothyronine',
            'O-(4-Hydroxy-3,5-diiodophenyl)-3,5-diiodo-L-tyrosine',
            'L-T4',
            'LT4',
            'T4',
            '3,3\',5,5\'-Tetraiodo-L-thyronine',
            'L-Thyroxine',
            'O-(4-Hydroxy-3,5-diidophenyl)-3,5-diiodo-L-tyrosine',
            '4-(4-Hydroxy-3,5-diiodophenoxy)-3,5-diiodo-L-phenylalanine',
            'Levothyroxin'
        ],
        'approval_status': 'approved',
        'xrefs': [
            'drugbank:DB00451'
        ],
        'associated_with': [
            'vandf:4022126',
            'mesh:D013974',
            'mmsl:d00278',
            'mthspl:Q51BO43MG4'
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


@pytest.fixture(scope='module')
def fluoxetine():
    """Create fluoxetine drug fixture."""
    params = {
        'label': 'fluoxetine',
        'concept_id': 'rxcui:4493',
        'aliases': [
            'FLUoxetine',
            'N-Methyl-gamma-(4-(trifluoromethyl)phenoxy)'
            'benzenepropanamine',
            'Fluoxetin',
            '(+-)-N-Methyl-gamma-(4-(trifluoromethyl)phenoxy)'
            'benzenepropanamine',
            '(+-)-N-Methyl-3-phenyl-3-((alpha,alpha,alpha-trifluoro-'
            'P-tolyl)oxy)propylamine'
        ],
        'approval_status': 'approved',
        'xrefs': [
            'drugbank:DB00472'
        ],
        'associated_with': [
            'mesh:D005473',
            'mmsl:17711',
            'vandf:4019761',
            'mmsl:d00236',
            'atc:N06AB03',
            'mthspl:01K63SUP8D'
        ],
        'trade_names': [
            'Prozac',
            'RECONCILE',
            'Sarafem',
            'Symbyax',
            'Rapiflux',
            'Selfemra'
        ]
    }
    return Drug(**params)


@pytest.fixture(scope='module')
def fluoxetine_hydrochloride():
    """Create fluoxetine hydrochloride drug fixture."""
    params = {
        'label': 'fluoxetine hydrochloride',
        'concept_id': 'rxcui:227224',
        'aliases': [
            'FLUoxetine hydrochloride',
            'Fluoxetine Hydrochloride'
        ],
        'approval_status': 'approved',
        'xrefs': [],
        'associated_with': [
            'usp:m33780',
            'mthspl:I9W7N6B1KJ',
            'mmsl:4746',
            'vandf:4019389',
            'mmsl:41730',
            'mmsl:37675',
            'mesh:D005473'
        ],
        'trade_names': [
            'FLUoxetine HCl',
            'FLUoxetine Hydrochloride',
            'RECONCILE'
        ]
    }
    return Drug(**params)


def test_bifidobacterium_infantis(bifidobacterium_infantis, rxnorm):
    """Test that bifidobacterium_ nfantis drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    response = rxnorm.search('RxCUI:100213')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)

    # Label Match
    response = rxnorm.search(' Bifidobacterium infantis')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)

    response = rxnorm.search('bifidobacterium infantis')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)

    # Trade Name Match
    response = rxnorm.search('ALIGN')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)

    response = rxnorm.search('evivo')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)

    # Xref Match
    response = rxnorm.search('drugbank:DB14222')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)


def test_cisplatin(cisplatin, rxnorm):
    """Test that cisplatin drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:2555')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    # Label Match
    response = rxnorm.search('CISPLATIN')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    # Alias Match
    response = rxnorm.search('Cis-DDP')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = rxnorm.search('Dichlorodiammineplatinum')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = rxnorm.search('cis Platinum')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    # Trade Name Match
    response = rxnorm.search('platinol')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    # Xref Match
    response = rxnorm.search('drugbank:DB12117')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)


def test_amiloride_hydrochloride(amiloride_hydrochloride, rxnorm):
    """Test that amiloride_hydrochloride hydrochloride, anhydrous drug
    normalizes to correct drug concept.
    """
    # Concept ID Match
    response = rxnorm.search('RxcUI:142424')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride_hydrochloride)

    # Label Match
    response = rxnorm.search('amiloride hydrochloride')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride_hydrochloride)

    # Trade Name Match
    response = rxnorm.search('Midamor')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 2


def test_amiloride(amiloride, rxnorm):
    """Test that amiloride hydrochloride, anhydrous drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    response = rxnorm.search('RxcUI:644')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride)

    # Label Match
    response = rxnorm.search('amiloride')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride)

    # Alias Match
    response = rxnorm.search('Amyloride')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride)

    response = rxnorm.search('3,5-diamino-N-carbamimidoyl-6-'
                             'chloropyrazine-2-carboxamide')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride)

    # Trade Name Match
    response = rxnorm.search('Midamor')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 2


def test_timolol(timolol, rxnorm):
    """Test that timolol drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxcUI:10600')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], timolol)

    # Label Match
    response = rxnorm.search('timolol')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], timolol)

    # Alias Match
    response = rxnorm.search('(S)-1-(tert-butylamino)-3-[(4-'
                             'morpholin-4-yl-1,2,5-thiadiazol'
                             '-3-yl)oxy]propan-2-ol')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], timolol)

    # Trade Name Match
    response = rxnorm.search('Betimol')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], timolol)


def test_lymphocyte(lymphocyte, rxnorm):
    """Test that lymphocyte drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:1011')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], lymphocyte)

    # Label Match
    response = rxnorm.search('lymphocyte immune globulin, '
                             'anti-thymocyte globulin')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], lymphocyte)

    # Alias Match
    response = rxnorm.search('Anti Thymocyte Globulin')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], lymphocyte)

    response = rxnorm.search('Antithymoglobulin')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], lymphocyte)

    # Trade Name Match
    response = rxnorm.search('Thymoglobulin')
    assert response['match_type'] == MatchType.TRADE_NAME

    response = rxnorm.search('ATGAM')
    assert response['match_type'] == MatchType.TRADE_NAME


def test_aspirin(aspirin, rxnorm):
    """Test that aspirin drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxcUI:1191')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1

    # (Trade Name) No Match
    response = rxnorm.search('Anacin')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0


def test_mesnan(mesna, rxnorm):
    """Test that mesna drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:44')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], mesna)

    # Label Match
    response = rxnorm.search('mesna')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], mesna)

    # Alias Match
    response = rxnorm.search('Mesnum')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], mesna)

    response = rxnorm.search('Ethanesulfonic acid, 2-mercapto-, '
                             'monosodium salt')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], mesna)

    # Trade Name Match
    response = rxnorm.search('Mesnex')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], mesna)


def test_beta_alanine(beta_alanine, rxnorm):
    """Test that beta_alanine drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:61')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], beta_alanine)

    # Label Match
    response = rxnorm.search('beta-alanine')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], beta_alanine)

    # Alias Match
    response = rxnorm.search('3 Aminopropionic Acid')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], beta_alanine)


def test_algestone(algestone, rxnorm):
    """Test that algestone drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:595')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], algestone)

    # Label Match
    response = rxnorm.search('algestone')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], algestone)

    # Alias Match
    response = rxnorm.search('Pregn-4-ene-3,20-dione, 16,'
                             '17-dihydroxy-, (16alpha)-')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], algestone)


def test_levothyroxine(levothyroxine, rxnorm):
    """Test that levothyroxine drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:10582')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)

    # Label Match
    response = rxnorm.search('levothyroxine')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)

    # Alias Match
    response = rxnorm.search('Thyroxin')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)

    response = rxnorm.search('LT4')
    assert response['match_type'] == MatchType.ALIAS
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)

    # Trade Name Match
    response = rxnorm.search('Unithroid')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)

    response = rxnorm.search('Euthyrox')
    assert response['match_type'] == MatchType.TRADE_NAME
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)

    # Xref Match
    response = rxnorm.search('DRUGBANK:DB00451')
    assert response['match_type'] == MatchType.XREF
    assert len(response['records']) == 1
    compare_records(response['records'][0], levothyroxine)


def test_fluoxetine(fluoxetine, rxnorm):
    """Test that fluoxetine drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search('RxCUI:4493')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], fluoxetine)

    # Label Match
    response = rxnorm.search('fluoxetine')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], fluoxetine)


def test_fluoxetine_hydrochloride(fluoxetine_hydrochloride, rxnorm):
    """Test that fluoxetine_hydrochloride drug normalizes to correct drug
    concept.
    """
    # Concept ID Match
    response = rxnorm.search('RxCUI:227224')
    assert response['match_type'] == MatchType.CONCEPT_ID
    assert len(response['records']) == 1
    compare_records(response['records'][0], fluoxetine_hydrochloride)

    # Label Match
    response = rxnorm.search('fluoxetine hydrochloride')
    assert response['match_type'] == MatchType.LABEL
    assert len(response['records']) == 1
    compare_records(response['records'][0], fluoxetine_hydrochloride)


def test_no_match(rxnorm):
    """Test that a term normalizes to correct drug concept as a NO match."""
    # Misspelled name
    response = rxnorm.search('17-hydroxycorticosteroi')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0

    # Not storing foreign synonyms
    response = rxnorm.search('cisplatino')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0

    # Wrong Namespace
    response = rxnorm.search('rxnorm:3')
    assert response['match_type'] == MatchType.NO_MATCH

    # Test white space in between id
    response = rxnorm.search('rxcui: 3')
    assert response['match_type'] == MatchType.NO_MATCH

    # Should not store brand name concepts as identity record
    response = rxnorm.search('rxcui:202433')
    assert response['match_type'] == MatchType.NO_MATCH

    # Test empty query
    response = rxnorm.search('')
    assert response['match_type'] == MatchType.NO_MATCH
    assert len(response['records']) == 0


def test_brand_name_to_concept(rxnorm):
    """Test that brand names are correctly linked to identity concept."""
    r = rxnorm.db.therapies.query(
        KeyConditionExpression=Key('label_and_type').eq(
            'rxcui:1041527##rx_brand')
    )
    assert r['Items'][0]['concept_id'] == 'rxcui:161'
    assert r['Items'][0]['concept_id'] != 'rxcui:1041527'

    r = rxnorm.db.therapies.query(
        KeyConditionExpression=Key('label_and_type').eq(
            'rxcui:218330##rx_brand')
    )
    assert r['Items'][0]['concept_id'] == 'rxcui:44'
    assert r['Items'][0]['concept_id'] != 'rxcui:218330'


def test_xref_lookup(rxnorm, bifidobacterium_infantis, cisplatin, amiloride):
    """Test that xref lookup resolves to correct concept."""
    response = rxnorm.search('mmsl:d07347')
    assert response['match_type'] == MatchType.ASSOCIATED_WITH
    assert len(response['records']) == 1
    compare_records(response['records'][0], bifidobacterium_infantis)

    response = rxnorm.search('mesh:D002945')
    assert response['match_type'] == MatchType.ASSOCIATED_WITH
    assert len(response['records']) == 1
    compare_records(response['records'][0], cisplatin)

    response = rxnorm.search('atc:C03DB01')
    assert response['match_type'] == MatchType.ASSOCIATED_WITH
    assert len(response['records']) == 1
    compare_records(response['records'][0], amiloride)


def test_meta_info(rxnorm):
    """Test that the meta field is correct."""
    response = rxnorm.fetch_meta()
    assert response.data_license == 'UMLS Metathesaurus'
    assert response.data_license_url == \
           'https://www.nlm.nih.gov/research/umls/rxnorm/docs/' \
           'termsofservice.html'
    assert response.version == '20210104'
    assert response.data_url == \
           'https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html'
    assert not response.rdp_url
    assert response.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True
    }
