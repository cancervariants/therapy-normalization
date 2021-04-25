"""Test the therapy querying method."""
from therapy.query import QueryHandler, InvalidParameterException
from therapy.schemas import MatchType
import pytest
from datetime import datetime


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
    class QueryGetter:
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
        "id": "normalize.therapy:Phenobarbital",
        "type": "TherapyDescriptor",
        "value": {
            "type": "Therapy",
            "id": "rxcui:8134"
        },
        "label": "phenobarbital",
        "xrefs": [
            "ncit:C739",
            "drugbank:DB01174",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
        ],
        "alternate_labels": [
            '5-Ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione',
            '5-Ethyl-5-phenyl-pyrimidine-2,4,6-trione',
            '5-Ethyl-5-phenylbarbituric acid',
            '5-Phenyl-5-ethylbarbituric acid',
            '5-ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione',
            '5-ethyl-5-phenylpyrimidine-2,4,6(1H,3H,5H)-trione',
            'APRD00184',
            'Acid, Phenylethylbarbituric',
            'Fenobarbital',
            'Luminal®',
            'PHENO',
            'PHENOBARBITAL',
            'PHENYLETHYLMALONYLUREA',
            'PHENobarbital',
            'Phenemal',
            'Phenobarbital',
            'Phenobarbitol',
            'Phenobarbitone',
            'Phenobarbituric Acid',
            'Phenylaethylbarbitursaeure',
            'Phenylbarbital',
            'Phenylethylbarbiturate',
            'Phenylethylbarbituric Acid',
            'Phenylethylbarbitursaeure',
            'Phenylethylbarbitursäure',
            'Phenylethylmalonylurea',
            'Phenyläthylbarbitursäure',
            'fenobarbital',
            'phenobarbital',
            'phenobarbital sodium',
            'phenylethylbarbiturate'
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "pubchem.compound:4763",
                    "usp:m63400",
                    "vandf:4017422",
                    "mmsl:2390",
                    "mesh:D010634",
                    "mmsl:5272",
                    "mthspl:YQE403BP4D",
                    "mmsl:d00340",
                    "atc:N03AA02",
                    "fda:YQE403BP4D",
                    "umls:C0031412",
                    "chebi:CHEBI:8069",
                    "chembl:CHEMBL40"
                ]
            },
        ]
    }


@pytest.fixture(scope='module')
def cisplatin():
    """Create cisplatin fixture."""
    return {
        "id": "normalize.therapy:Cisplatin",
        "type": "TherapyDescriptor",
        "value": {
            "type": "Therapy",
            "id": "rxcui:2555",
        },
        "label": "cisplatin",
        "xrefs": [
            "ncit:C376",
            "hemonc:105",
            "drugbank:DB00515",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001"
        ],
        "alternate_labels": [
            '1,2-Diaminocyclohexaneplatinum II citrate',
            'APRD00359',
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
            'Platinol',
            'cisplatino',
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "trade_names",
                "value": [
                    "Cisplatin",
                    "Platinol"
                ],
            },
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "mthspl:Q20Q21Q62J",
                    "mmsl:31747",
                    "mmsl:4456",
                    "usp:m17910",
                    "chebi:CHEBI:27899",
                    "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L",
                    "mmsl:d00195",
                    "unii:Q20Q21Q62J",
                    "mesh:D002945",
                    "atc:L01XA01",
                    "vandf:4018139",
                    "pubchem.compound:5702198",
                    "umls:C0008838",
                    "fda:Q20Q21Q62J",
                    "chembl:CHEMBL11359",
                ]
            },
            {
                "type": "Extension",
                "name": "fda_approval",
                "value": {
                    "approval_status": "approved",
                    "approval_year": ["1978"],
                    "has_indication": [
                        {
                            "id": "hemonc:671",
                            "type": "DiseaseDescriptor",
                            "value": {
                                "type": "Disease",
                                "id": "ncit:C7251"
                            },
                            "label": "Testicular cancer"
                        },
                        {
                            "id": "hemonc:645",
                            "type": "DiseaseDescriptor",
                            "value": {
                                "type": "Disease",
                                "id": "ncit:C7431"
                            },
                            "label": "Ovarian cancer"
                        },
                        {
                            "id": "hemonc:569",
                            "type": "DiseaseDescriptor",
                            "value": {
                                "type": "Disease",
                                "id": "ncit:C9334"
                            },
                            "label": "Bladder cancer"
                        }
                    ]
                }
            },
        ]
    }


@pytest.fixture(scope='module')
def spiramycin():
    """Create fixture for spiramycin."""
    return {
        "id": "normalize.therapy:Spiramycin",
        "type": "TherapyDescriptor",
        "value": {
            "type": "Therapy",
            "id": "ncit:C839"
        },
        "label": "Spiramycin",
        "xrefs": [
            "chemidplus:8025-81-8"
        ],
        "alternate_labels": [
            "SPIRAMYCIN",
            "Antibiotic 799",
            "Rovamycin",
            "Provamycin",
            "Rovamycine",
            "RP 5337",
            "(4R,5S,6R,7R,9R,10R,11E,13E,16R)-10-{[(2R,5S,6R)-5-(dimethylamino)-6-methyltetrahydro-2H-pyran-2-yl]oxy}-9,16-dimethyl-5-methoxy-2-oxo-7-(2-oxoethyl)oxacyclohexadeca-11,13-dien-6-yl 3,6-dideoxy-4-O-(2,6-dideoxy-3-C-methyl-alpha-L-ribo-hexopyranosyl)-3-(dimethylamino)-alpha-D-glucopyranoside"  # noqa: E501
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "umls:C0037962",
                    "fda:71ODY0V87H",
                ],
            }
        ]
    }


@pytest.fixture(scope='module')
def timolol():
    """Create fixture for timolol."""
    return {
        "id": "normalize.therapy:timolol",
        "type": "TherapyDescriptor",
        "value": {
            "type": "Therapy",
            "id": "rxcui:10600"
        },
        "label": "timolol",
        "alternate_labels": [
            "(S)-1-((1,1-Dimethylethyl)amino)-3-((4-(4-morpholinyl)-1,2,5-thiadazol-3-yl)oxy)-2-propanol",  # noqa: E501
            "2-Propanol, 1-((1,1-dimethylethyl)amino)-3-((4-(4-morpholinyl)-1,2,5-thiadiazol-3-yl)oxy)-, (S)-",  # noqa: E501
            "(S)-1-(tert-butylamino)-3-[(4-morpholin-4-yl-1,2,5-thiadiazol-3-yl)oxy]propan-2-ol"  # noqa: E501
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "trade_names",
                "value": [
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
                ]
            },
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "atc:C07AA06",
                    "mthspl:817W3C6175",
                    "vandf:4019949",
                    "atc:S01ED01",
                    "mesh:D013999",
                    "mmsl:d00139"
                ]
            }
        ]
    }


def compare_vod(actual, fixture):
    """Verify correctness of returned VOD object against test fixture."""
    assert actual['id'] == fixture['id']
    assert actual['type'] == fixture['type']
    assert actual['value'] == fixture['value']
    assert actual['label'] == fixture['label']

    assert ('xrefs' in actual.keys()) == ('xrefs' in fixture.keys())
    if 'xrefs' in actual:
        assert set(actual['xrefs']) == set(fixture['xrefs'])

    assert ('alternate_labels' in actual.keys()) == ('alternate_labels' in fixture.keys())  # noqa: E501
    if 'alternate_labels' in actual:
        assert set(actual['alternate_labels']) == \
            set(fixture['alternate_labels'])

    def get_extension(extensions, name):
        matches = [e for e in extensions if e['name'] == name]
        if len(matches) > 1:
            assert False
        elif len(matches) == 1:
            return matches[0]
        else:
            return None

    assert ('extensions' in actual.keys()) == ('extensions' in fixture.keys())  # noqa: E501
    if 'extensions' in actual:
        ext_actual = actual['extensions']
        ext_fixture = fixture['extensions']

        assoc_actual = get_extension(ext_actual, 'associated_with')
        assoc_fixture = get_extension(ext_fixture, 'associated_with')
        assert (assoc_actual is None) == (assoc_fixture is None)
        if assoc_actual:
            assert set(assoc_actual['value']) == set(assoc_fixture['value'])
            assert assoc_actual['value']

        tn_actual = get_extension(ext_actual, 'trade_names')
        tn_fixture = get_extension(ext_fixture, 'trade_names')
        assert (tn_actual is None) == (tn_fixture is None)
        if tn_actual:
            assert set(tn_actual['value']) == set(tn_fixture['value'])
            assert tn_actual['value']

        fda_actual = get_extension(ext_actual, 'fda_approval')
        fda_fixture = get_extension(ext_fixture, 'fda_approval')
        assert (fda_actual is None) == (fda_fixture is None)
        if fda_actual:
            assert fda_actual.get('approval_status') == \
                fda_fixture.get('approval_status')
            assert set(fda_actual.get('approval_year', [])) == \
                set(fda_fixture.get('approval_year', []))
            assert set(fda_actual.get('has_indication', [])) == \
                set(fda_fixture.get('has_indication', []))


def test_query(query_handler):
    """Test that query returns properly-structured response."""
    resp = query_handler.normalize('cisplatin', keyed=False)
    assert resp['query'] == 'cisplatin'
    matches = resp['source_matches']
    assert isinstance(matches, list)
    assert len(matches) == 7
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


def test_query_specify_sources(query_handler):
    """Test inclusion and exclusion of sources in query."""
    # test blank params
    resp = query_handler.normalize('cisplatin', keyed=True)
    matches = resp['source_matches']
    assert len(matches) == 7
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches
    assert 'ChemIDplus' in matches
    assert 'RxNorm' in matches
    assert 'HemOnc' in matches

    # test partial inclusion
    resp = query_handler.normalize('cisplatin', keyed=True, incl='chembl,ncit')
    matches = resp['source_matches']
    assert len(matches) == 2
    assert 'Wikidata' not in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' not in matches
    assert 'RxNorm' not in matches
    assert 'ChemIDplus' not in matches
    assert 'HemOnc' not in matches

    # test full inclusion
    sources = 'chembl,ncit,drugbank,wikidata,rxnorm,chemidplus,hemonc'
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   incl=sources, excl='')
    matches = resp['source_matches']
    assert len(matches) == 7
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches
    assert 'ChemIDplus' in matches
    assert 'RxNorm' in matches
    assert 'HemOnc' in matches

    # test partial exclusion
    resp = query_handler.normalize('cisplatin', keyed=True, excl='chemidplus')
    matches = resp['source_matches']
    assert len(matches) == 6
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches
    assert 'ChemIDplus' not in matches
    assert 'RxNorm' in matches
    assert 'HemOnc' in matches

    # test full exclusion
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   excl='chembl, wikidata, drugbank, ncit, '
                                   'rxnorm, chemidplus, hemonc')
    matches = resp['source_matches']
    assert len(matches) == 0
    assert 'Wikidata' not in matches
    assert 'ChEMBL' not in matches
    assert 'NCIt' not in matches
    assert 'DrugBank' not in matches
    assert 'ChemIDplus' not in matches
    assert 'RxNorm' not in matches
    assert 'HemOnc' not in matches

    # test case insensitive
    resp = query_handler.normalize('cisplatin', keyed=True, excl='ChEmBl')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' not in matches
    assert 'NCIt' in matches
    assert 'DrugBank' in matches
    assert 'ChemIDplus' in matches
    assert 'RxNorm' in matches
    assert 'HemOnc' in matches
    resp = query_handler.normalize('cisplatin', keyed=True,
                                   incl='wIkIdAtA,cHeMbL')
    matches = resp['source_matches']
    assert 'Wikidata' in matches
    assert 'ChEMBL' in matches
    assert 'NCIt' not in matches
    assert 'DrugBank' not in matches
    assert 'ChemIDplus' not in matches
    assert 'RxNorm' not in matches
    assert 'HemOnc' not in matches

    # test error on invalid source names
    with pytest.raises(InvalidParameterException):
        resp = query_handler.normalize('cisplatin', keyed=True, incl='chambl')

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterException):
        resp = query_handler.normalize('cisplatin', keyed=True, incl='chembl',
                                       excl='wikidata')


def test_query_merged(merge_query_handler, phenobarbital, cisplatin,
                      spiramycin, timolol):
    """Test that the merged concept endpoint handles queries correctly."""
    # test merged id match
    query = "rxcui:2555"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.CONCEPT_ID
    cisplatin_copy = cisplatin.copy()
    cisplatin_copy['id'] = 'normalize.therapy:rxcui%3A2555'
    compare_vod(response['value_object_descriptor'], cisplatin_copy)

    # test concept id match
    query = "chemidplus:50-06-6"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.CONCEPT_ID
    pheno_copy = phenobarbital.copy()
    pheno_copy['id'] = 'normalize.therapy:chemidplus%3A50-06-6'
    compare_vod(response['value_object_descriptor'], pheno_copy)

    query = "hemonc:105"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.CONCEPT_ID
    cisplatin_copy = cisplatin.copy()
    cisplatin_copy['id'] = 'normalize.therapy:hemonc%3A105'
    compare_vod(response['value_object_descriptor'], cisplatin_copy)

    # test label match
    query = "Phenobarbital"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.LABEL
    compare_vod(response['value_object_descriptor'], phenobarbital)

    # test trade name match
    query = "Platinol"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.TRADE_NAME
    cisplatin_copy = cisplatin.copy()
    cisplatin_copy['id'] = 'normalize.therapy:Platinol'
    compare_vod(response['value_object_descriptor'], cisplatin_copy)

    # test alias match
    query = "cis Diamminedichloroplatinum"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.ALIAS
    cisplatin_copy = cisplatin.copy()
    cisplatin_copy['id'] = 'normalize.therapy:cis%20Diamminedichloroplatinum'
    compare_vod(response['value_object_descriptor'], cisplatin_copy)

    query = "Rovamycine"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.ALIAS
    spiramycin_copy = spiramycin.copy()
    spiramycin_copy['id'] = 'normalize.therapy:Rovamycine'
    compare_vod(response['value_object_descriptor'], spiramycin_copy)

    # test merge group with single member
    query = "Betimol"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.TRADE_NAME
    timolol_copy = timolol.copy()
    timolol_copy['id'] = 'normalize.therapy:Betimol'
    compare_vod(response['value_object_descriptor'], timolol_copy)

    # test that term with multiple possible resolutions resolves at highest
    # match
    query = "Cisplatin"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.LABEL
    compare_vod(response['value_object_descriptor'], cisplatin)

    # test whitespace stripping
    query = "   Cisplatin "
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert response['match_type'] == MatchType.LABEL
    compare_vod(response['value_object_descriptor'], cisplatin)

    # test no match
    query = "zzzz fake therapy zzzz"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert 'record' not in response
    assert response['match_type'] == MatchType.NO_MATCH

    query = "APRD00818"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert 'record' not in response
    assert response['match_type'] == MatchType.NO_MATCH

    query = "chembl:CHEMBL1200368"
    response = merge_query_handler.search_groups(query)
    assert response['query'] == query
    assert response['warnings'] is None
    assert 'record' not in response
    assert response['match_type'] == MatchType.NO_MATCH


def test_merged_meta(merge_query_handler):
    """Test population of source and resource metadata in merged querying."""
    query = "pheno"
    response = merge_query_handler.search_groups(query)
    meta_items = response['source_meta_']
    assert 'RxNorm' in meta_items.keys()
    assert 'Wikidata' in meta_items.keys()
    assert 'NCIt' in meta_items.keys()
    assert 'ChemIDplus' in meta_items.keys()

    query = "RP 5337"
    response = merge_query_handler.search_groups(query)
    meta_items = response['source_meta_']
    assert 'NCIt' in meta_items.keys()
    assert 'ChemIDplus' in meta_items.keys()


def test_service_meta(query_handler, merge_query_handler):
    """Test service meta info in response."""
    query = "pheno"

    response = query_handler.normalize(query)
    service_meta = response['service_meta_']
    assert service_meta.name == "thera-py"
    assert service_meta.version >= "0.2.13"
    assert isinstance(service_meta.response_datetime, datetime)
    assert service_meta.url == 'https://github.com/cancervariants/therapy-normalization'  # noqa: E501

    response = merge_query_handler.search_groups(query)
    service_meta = response['service_meta_']
    assert service_meta.name == "thera-py"
    assert service_meta.version >= "0.2.13"
    assert isinstance(service_meta.response_datetime, datetime)
    assert service_meta.url == 'https://github.com/cancervariants/therapy-normalization'  # noqa: E501
