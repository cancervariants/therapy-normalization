"""Test the therapy querying method."""
from therapy.query import QueryHandler, InvalidParameterException
from therapy.schemas import MatchType
import pytest


@pytest.fixture(scope='module')
def query_handler():
    """Build query handler test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def normalize(self, query_str, keyed=False, incl='', excl=''):
            resp = self.query_handler.search_sources(query_str=query_str,
                                                     keyed=keyed,
                                                     incl=incl, excl=excl)
            return resp

    return QueryGetter()


@pytest.fixture(scope='module')
def merge_query_handler(mock_database):
    """Provide Merge instance to test cases."""
    class QueryGetter():
        def __init__(self):
            self.query_handler = QueryHandler(db_url='http://localhost:8000')
            self.query_handler.db = mock_database()  # replace initial DB

        def search_groups(self, query_str):
            return self.query_handler.search_groups(query_str)

    return QueryGetter()


@pytest.fixture(scope='module')
def phenobarbital():
    """Create phenobarbital fixture."""
    return {
        "concept_ids": [
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241"
        ],
        "aliases": [
            '5-Ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione',
            '5-Ethyl-5-phenyl-pyrimidine-2,4,6-trione',
            '5-Ethyl-5-phenylbarbituric acid',
            '5-Phenyl-5-ethylbarbituric acid',
            '5-ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione',
            '5-ethyl-5-phenylpyrimidine-2,4,6(1H,3H,5H)-trione',
            'Acid, Phenylethylbarbituric',
            'Luminal®',
            'PHENO',
            'PHENOBARBITAL',
            'PHENYLETHYLMALONYLUREA',
            'PHENobarbital',
            'Phenemal',
            'Phenobarbital',
            'Phenobarbital (substance)',
            'Phenobarbital-containing product',
            'Phenobarbitol',
            'Phenobarbitone',
            'Phenobarbituric Acid',
            'Phenylaethylbarbitursaeure',
            'Phenylbarbital',
            'Phenylethylbarbiturate',
            'Phenylethylbarbituric Acid',
            'Phenylethylbarbitursaeure',
            'Phenylethylmalonylurea',
            'Product containing phenobarbital (medicinal product)',
            'fenobarbital',
            'phenobarbital',
            'phenobarbital sodium',
            'phenylethylbarbiturate'
        ],
        "trade_names": [
            "Phenobarbital",
        ],
        "xrefs": [
            "pubchem.compound:4763",
            "usp:m63400",
            "gsddb:2179",
            "snomedct:51073002",
            "vandf:4017422",
            "mmsl:2390",
            "msh:D010634",
            "snomedct:373505007",
            "mmsl:5272",
            "mthspl:YQE403BP4D",
            "fdbmk:001406",
            "mmsl:d00340",
            "atc:N03AA02",
            "fda:YQE403BP4D",
            "umls:C0031412",
            "chebi:CHEBI:8069"

        ],
        "label": "Phenobarbital"
    }


@pytest.fixture(scope='module')
def cisplatin():
    """Create cisplatin fixture."""
    return {
        "concept_ids": [
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001"
        ],
        "trade_names": [
            "Cisplatin",
            "Platinol"
        ],
        "aliases": [
            '1,2-Diaminocyclohexaneplatinum II citrate',
            'CDDP',
            'CISplatin',
            'Cis-DDP',
            'CIS-DDP',
            'DDP',
            'Diamminodichloride, Platinum',
            'Dichlorodiammineplatinum',
            'Platinum Diamminodichloride',
            'cis Diamminedichloroplatinum',
            'cis Platinum',
            'cis-Diaminedichloroplatinum',
            'cis-Diamminedichloroplatinum',
            'cis-Diamminedichloroplatinum(II)',
            'cis-Dichlorodiammineplatinum(II)',
            'cis-Platinum',
            'cis-diamminedichloroplatinum(II)',
            'Platinol-AQ',
            'Platinol'
        ],
        "label": "cisplatin",
        "xrefs": [
            "umls:C0008838",
            "fda:Q20Q21Q62J",
            "usp:m17910",
            "vandf:4018139",
            "mesh:D002945",
            "mthspl:Q20Q21Q62J",
            "mmsl:d00195",
            "atc:L01XA01",
            "mmsl:31747",
            "mmsl:4456",
            "pubchem.compound:5702198"
        ]
    }


@pytest.fixture(scope='module')
def spiramycin():
    """Create fixture for spiramycin."""
    return {
        "concept_ids": [
            "ncit:C839",
            "chemidplus:8025-81-8"
        ],
        "label": "Spiramycin",
        "aliases": [
            "SPIRAMYCIN",
            "Antibiotic 799",
            "Rovamycin",
            "Provamycin",
            "Rovamycine",
            "RP 5337",
            "(4R,5S,6R,7R,9R,10R,11E,13E,16R)-10-{[(2R,5S,6R)-5-(dimethylamino)-6-methyltetrahydro-2H-pyran-2-yl]oxy}-9,16-dimethyl-5-methoxy-2-oxo-7-(2-oxoethyl)oxacyclohexadeca-11,13-dien-6-yl 3,6-dideoxy-4-O-(2,6-dideoxy-3-C-methyl-alpha-L-ribo-hexopyranosyl)-3-(dimethylamino)-alpha-D-glucopyranoside"],  # noqa: E501
        "xrefs": [
            "umls:C0037962",
            "fda:71ODY0V87H",
        ],
    }


@pytest.fixture(scope='module')
def timolol():
    """Create fixture for timolol."""
    return {
        "concept_ids": ["rxcui:10600"],
        "aliases": [
            "(S)-1-((1,1-Dimethylethyl)amino)-3-((4-(4-morpholinyl)-1,2,5-thiadazol-3-yl)oxy)-2-propanol",  # noqa: E501
            "2-Propanol, 1-((1,1-dimethylethyl)amino)-3-((4-(4-morpholinyl)-1,2,5-thiadiazol-3-yl)oxy)-, (S)-",  # noqa: E501
            "(S)-1-(tert-butylamino)-3-[(4-morpholin-4-yl-1,2,5-thiadiazol-3-yl)oxy]propan-2-ol"  # noqa: E501
        ],
        "trade_names": [
            "Timoptol",
            "Timolide",
            "Betimol",
            "Betim",
            "Timoptic",
            "Combigan",
            "Istalol",
            "Glaucol",
            "Cosopt",
            "Blocadren",
            "Timoptol LA",
            "Glau-Opt"
        ],
        "xrefs": [
            "atc:C07AA06",
            "mthspl:817W3C6175",
            "vandf:4019949",
            "atc:S01ED01",
            "mesh:D013999",
            "mmsl:d00139"
        ],
        "label": "timolol"
    }


def test_query(query_handler):
    """Test that query returns properly-structured response."""
    resp = query_handler.normalize('cisplatin', keyed=False)
    assert resp['query'] == 'cisplatin'
    matches = resp['source_matches']
    assert isinstance(matches, list)
    assert len(matches) == 6
    wikidata = list(filter(lambda m: m['source'] == 'Wikidata',
                           matches))[0]
    assert len(wikidata['records']) == 1
    wikidata_record = wikidata['records'][0]
    assert wikidata_record.label == 'cisplatin'


def test_query_keyed(query_handler):
    """Test that query structures matches as dict when requested."""
    resp = query_handler.normalize('penicillin v', keyed=True)
    matches = resp['source_matches']
    assert isinstance(matches, dict)
    chemidplus = matches['ChemIDplus']
    chemidplus_record = chemidplus['records'][0]
    assert chemidplus_record.label == 'Penicillin V'


def test_query_specify_query_handlers(query_handler):
    """Test inclusion and exclusion of sources in query."""
    # test blank params
    resp = query_handler.normalize('cisplatin', keyed=True)
    matches = resp['source_matches']
    assert len(matches) == 6
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches

    # test partial inclusion
    resp = query_handler.normalize('cisplatin', keyed=True, incl='chembl,ncit')
    matches = resp['source_matches']
    assert len(matches) == 2
    assert 'Wikidata' not in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' not in matches

    # test full inclusion
    sources = 'chembl,ncit,drugbank,wikidata,rxnorm,chemidplus'
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   incl=sources, excl='')
    matches = resp['source_matches']
    assert len(matches) == 6
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test partial exclusion
    resp = query_handler.normalize('cisplatin', keyed=True, excl='chemidplus')
    matches = resp['source_matches']
    assert len(matches) == 5
    assert 'Wikidata' in matches
    assert 'ChemIDplus' not in matches

    # test full exclusion
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   excl='chembl, wikidata, drugbank, ncit, '
                                   'rxnorm, chemidplus')
    matches = resp['source_matches']
    assert len(matches) == 0
    assert 'Wikidata' not in matches
    assert 'ChEMBL' not in matches
    assert 'NCIt' not in matches
    assert 'DrugBank' not in matches

    # test case insensitive
    resp = query_handler.normalize('cisplatin', keyed=True, excl='ChEmBl')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' not in matches
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   incl='wIkIdAtA,cHeMbL')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches

    # test error on invalid source names
    with pytest.raises(InvalidParameterException):
        resp = query_handler.normalize('cisplatin', keyed=True, incl='chambl')

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterException):
        resp = query_handler.normalize('cisplatin', keyed=True, incl='chembl',
                                       excl='wikidata')


def compare_records(test_record, record_fixture):
    """Assert equality of test record against correct record.

    :param Dict test_record: record produced in test case
    :param Dict record_fixture: expected record output
    """
    assert test_record['concept_ids'] == record_fixture['concept_ids']
    if 'aliases' in test_record or 'aliases' in record_fixture:
        assert set(test_record['aliases']) == set(record_fixture['aliases'])
    if 'trade_names' in test_record or 'trade_names' in record_fixture:
        assert set(test_record['trade_names']) == set(record_fixture['trade_names'])  # noqa: E501
    if 'xrefs' in test_record or 'xrefs' in record_fixture:
        assert set(test_record['xrefs']) == set(record_fixture['xrefs'])
    assert test_record['label'] == record_fixture['label']


def test_query_merged(merge_query_handler, phenobarbital, cisplatin,
                      spiramycin, timolol):
    """Test that the merged concept endpoint handles queries correctly."""
    # test merged id match
    test_query = "rxcui:2555|ncit:C376|chemidplus:15663-27-1|wikidata:Q412415|wikidata:Q47522001"  # noqa: E501
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.CONCEPT_ID
    compare_records(response['record'], cisplatin)

    # test concept id match
    test_query = "chemidplus:50-06-6"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.CONCEPT_ID
    compare_records(response['record'], phenobarbital)

    # # test label match
    test_query = "phenobarbital"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.LABEL
    compare_records(response['record'], phenobarbital)

    # test trade name match
    test_query = "Platinol"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.TRADE_NAME
    compare_records(response['record'], cisplatin)

    # test alias match
    test_query = "cis Diamminedichloroplatinum"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.ALIAS
    compare_records(response['record'], cisplatin)

    test_query = "Rovamycine"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.ALIAS
    compare_records(response['record'], spiramycin)

    # test no match
    test_query = "zzzz fake therapy zzzz"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert 'record' not in response
    assert response['match_type'] == MatchType.NO_MATCH

    # test merge group with single member
    test_query = "Betimol"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.TRADE_NAME
    compare_records(response['record'], timolol)

    # test that term with multiple possible resolutions resolves at highest
    # match
    test_query = "Cisplatin"
    response = merge_query_handler.search_groups(test_query)
    assert response['query'] == test_query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.LABEL
    compare_records(response['record'], cisplatin)


def test_merged_meta(merge_query_handler):
    """Test population of source metadata in merged querying."""
    test_query = "pheno"
    response = merge_query_handler.search_groups(test_query)
    meta_items = response['meta_']
    assert 'RxNorm' in meta_items.keys()
    assert 'Wikidata' in meta_items.keys()
    assert 'NCIt' in meta_items.keys()
    assert 'ChemIDplus' in meta_items.keys()

    test_query = "RP 5337"
    response = merge_query_handler.search_groups(test_query)
    meta_items = response['meta_']
    assert 'NCIt' in meta_items.keys()
    assert 'ChemIDplus' in meta_items.keys()
