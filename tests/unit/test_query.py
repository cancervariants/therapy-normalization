"""Test the therapy querying method."""
from datetime import datetime
import os

import pytest

from therapy.query import QueryHandler, InvalidParameterException
from therapy.schemas import MatchType


@pytest.fixture(scope="module")
def query_handler():
    """Build query handler test fixture."""
    class QueryGetter:

        def __init__(self):
            self.query_handler = QueryHandler()

        def search_sources(self, query_str, keyed=False, incl="", excl="",
                           infer=True):
            resp = self.query_handler.search_sources(query_str=query_str,
                                                     keyed=keyed,
                                                     incl=incl, excl=excl,
                                                     infer=infer)
            return resp

    return QueryGetter()


@pytest.fixture(scope="module")
def merge_query_handler(mock_database):
    """Provide Merge instance to test cases."""
    class QueryGetter:
        def __init__(self):
            self.query_handler = QueryHandler()
            if os.environ.get("TEST") is not None:
                self.query_handler.db = mock_database()  # replace initial DB

        def search_groups(self, query_str, infer=True):
            return self.query_handler.search_groups(query_str, infer)

    return QueryGetter()


@pytest.fixture(scope="module")
def phenobarbital():
    """Create phenobarbital VOD fixture."""
    return {
        "id": "normalize.therapy:Phenobarbital",
        "type": "TherapyDescriptor",
        "therapy_id": "rxcui:8134",
        "label": "phenobarbital",
        "xrefs": [
            "ncit:C739",
            "drugbank:DB01174",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "iuphar.ligand:2804",
            "chembl:CHEMBL40"
        ],
        "alternate_labels": [
            "5-Ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione",
            "5-Ethyl-5-phenyl-pyrimidine-2,4,6-trione",
            "5-Ethyl-5-phenylbarbituric acid",
            "5-Phenyl-5-ethylbarbituric acid",
            "5-ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione",
            "5-ethyl-5-phenylpyrimidine-2,4,6(1H,3H,5H)-trione",
            "APRD00184",
            "Acid, Phenylethylbarbituric",
            "Fenobarbital",
            "Luminal®",
            "PHENO",
            "PHENOBARBITAL",
            "PHENYLETHYLMALONYLUREA",
            "PHENobarbital",
            "Phenemal",
            "Phenobarbital",
            "Phenobarbitol",
            "Phenobarbitone",
            "Phenobarbituric Acid",
            "Phenylaethylbarbitursaeure",
            "Phenylbarbital",
            "Phenylethylbarbiturate",
            "Phenylethylbarbituric Acid",
            "Phenylethylbarbitursaeure",
            "Phenylethylbarbitursäure",
            "Phenylethylmalonylurea",
            "Phenyläthylbarbitursäure",
            "fenobarbital",
            "phenobarbital",
            "phenobarbital sodium",
            "phenylethylbarbiturate",
            "5-ethyl-5-phenyl-1,3-diazinane-2,4,6-trione",
            "phenobarb",
            "phenobarbitone",
            "NSC-128143",
            "NSC-128143-",
            "NSC-9848",
            "Phenobarbital civ",
            "Solfoton",
            "Luminal",
            "Eskabarb",
            "Noptil",
            "Talpheno"
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "usp:m63400",
                    "vandf:4017422",
                    "mmsl:2390",
                    "mesh:D010634",
                    "mmsl:5272",
                    "mmsl:d00340",
                    "atc:N03AA02",
                    "unii:YQE403BP4D",
                    "umls:C0031412",
                    "CHEBI:8069",
                    "pubchem.substance:135650817",
                    "pubchem.compound:4763",
                    "inchikey:DDBREPKUVSBGFI-UHFFFAOYSA-N",
                    "drugcentral:2134"
                ]
            },
            {
                "type": "Extension",
                "name": "regulatory_approval",
                "value": {
                    "approval_ratings": [],
                    "approval_year": [],
                    "has_indication": []
                }
            }
        ]
    }


@pytest.fixture(scope="module")
def cisplatin():
    """Create cisplatin fixture."""
    return {
        "id": "normalize.therapy:Cisplatin",
        "type": "TherapyDescriptor",
        "therapy_id": "rxcui:2555",
        "label": "cisplatin",
        "xrefs": [
            "ncit:C376",
            "hemonc:105",
            "drugbank:DB00515",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "iuphar.ligand:5343",
            "drugbank:DB12117",
            "chembl:CHEMBL11359",
            "drugsatfda.anda:074656",
            "drugsatfda.anda:074735",
            "drugsatfda.anda:206774",
            "drugsatfda.anda:207323",
            "drugsatfda.anda:075036",
            "drugsatfda.nda:018057",
        ],
        "alternate_labels": [
            "1,2-Diaminocyclohexaneplatinum II citrate",
            "APRD00359",
            "CDDP",
            "CISplatin",
            "Cis-DDP",
            "CIS-DDP",
            "DDP",
            "Diamminodichloride, Platinum",
            "Dichlorodiammineplatinum",
            "Platinum Diamminodichloride",
            "cis Diamminedichloroplatinum",
            "cis Platinum",
            "cis-Diaminedichloroplatinum",
            "cis-Diamminedichloroplatinum",
            "cis-Diamminedichloroplatinum(II)",
            "cis-Dichlorodiammineplatinum(II)",
            "cis-Platinum",
            "cis-diamminedichloroplatinum(II)",
            "Platinol-AQ",
            "Platinol",
            "cisplatino",
            "Cisplatin",
            "Cis-platinum ii",
            "Cisplatinum",
            "INT-230-6 COMPONENT CISPLATIN",
            "INT230-6 COMPONENT CISPLATIN",
            "NSC-119875",
            "Platinol-aq",
            "DACP",
            "NSC 119875",
            "cis-diamminedichloroplatinum III",
            "cis-platinum",
            "cisplatinum",
            "Liplacis"
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "trade_names",
                "value": [
                    "Cisplatin",
                    "Platinol",
                    "PLATINOL",
                    "PLATINOL-AQ"
                ],
            },
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "mmsl:31747",
                    "mmsl:4456",
                    "mmsl:d00195",
                    "usp:m17910",
                    "CHEBI:27899",
                    "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L",
                    "inchikey:MOTIYCLHZZLHHQ-UHFFFAOYSA-N",
                    "mesh:D002945",
                    "atc:L01XA01",
                    "vandf:4018139",
                    "pubchem.substance:178102005",
                    "pubchem.compound:441203",
                    "pubchem.compound:5702198",
                    "umls:C0008838",
                    "unii:H8MTN7XVC2",
                    "unii:Q20Q21Q62J",
                    "ndc:0143-9504",
                    "ndc:0143-9505",
                    "ndc:0703-5747",
                    "ndc:0703-5748",
                    "ndc:16729-288",
                    "ndc:44567-509",
                    "ndc:44567-510",
                    "ndc:44567-511",
                    "ndc:44567-530",
                    "ndc:63323-103",
                    "ndc:68001-283",
                    "ndc:68083-162",
                    "ndc:68083-163",
                    "ndc:70860-206",
                    "spl:01c7a680-ee0d-42da-85e8-8d56c6fe7006",
                    "spl:5a24d5bd-c44a-43f7-a04c-76caf3475012",
                    "spl:a66eda32-1164-439a-ac8e-73138365ec06",
                    "spl:dd45d777-d4c1-40ee-b4f0-c9e001a15a8c",
                    "spl:2c569ef0-588f-4828-8b2d-03a2120c9b4c",
                    "spl:54b3415c-c095-4c82-b216-e0e6e6bb8d03",
                    "spl:9b008181-ab66-db2f-e053-2995a90aad57",
                    "spl:c3ddc4a5-9f1b-a8ee-e053-2a95a90a2265",
                    "spl:c43de769-d6d8-3bb9-e053-2995a90a5aa2"
                ]
            },
            {
                "type": "Extension",
                "name": "regulatory_approval",
                "value": {
                    "approval_ratings": [],
                    "approval_year": ["1978"],
                    "has_indication": [
                        {
                            "id": "hemonc:671",
                            "type": "DiseaseDescriptor",
                            "disease_id": "ncit:C7251",
                            "label": "Testicular cancer"
                        },
                        {
                            "id": "hemonc:645",
                            "type": "DiseaseDescriptor",
                            "disease_id": "ncit:C7431",
                            "label": "Ovarian cancer"
                        },
                        {
                            "id": "hemonc:569",
                            "type": "DiseaseDescriptor",
                            "disease_id": "ncit:C9334",
                            "label": "Bladder cancer"
                        }
                    ]
                }
            }
        ]
    }


@pytest.fixture(scope="module")
def spiramycin():
    """Create fixture for spiramycin."""
    return {
        "id": "normalize.therapy:Spiramycin",
        "type": "TherapyDescriptor",
        "therapy_id": "ncit:C839",
        "label": "Spiramycin",
        "xrefs": [
            "chemidplus:8025-81-8",
            "wikidata:Q422265"
        ],
        "alternate_labels": [
            "SPIRAMYCIN",
            "Antibiotic 799",
            "Rovamycin",
            "Provamycin",
            "Rovamycine",
            "RP 5337",
            "(4R,5S,6R,7R,9R,10R,11E,13E,16R)-10-{[(2R,5S,6R)-5-(dimethylamino)-6-methyltetrahydro-2H-pyran-2-yl]oxy}-9,16-dimethyl-5-methoxy-2-oxo-7-(2-oxoethyl)oxacyclohexadeca-11,13-dien-6-yl 3,6-dideoxy-4-O-(2,6-dideoxy-3-C-methyl-alpha-L-ribo-hexopyranosyl)-3-(dimethylamino)-alpha-D-glucopyranoside",  # noqa: E501
            "spiramycin I"
        ],
        "extensions": [
            {
                "type": "Extension",
                "name": "associated_with",
                "value": [
                    "umls:C0037962",
                    "unii:71ODY0V87H",
                    "pubchem.compound:5356392"
                ]
            }
        ]
    }


@pytest.fixture(scope="module")
def therapeutic_procedure():
    """Create a fixture for the Therapeutic Procedure concept. Used to validate
    single-member concept groups for the normalize endpoint.
    """
    return {
        "id": "normalize.therapy:ncit%3AC49236",
        "therapy_id": "ncit:C49236",
        "label": "Therapeutic Procedure",
        "alternate_labels": [
            "any therapy",
            "any_therapy",
            "Therapeutic Interventions",
            "Therapeutic Method",
            "Therapeutic Technique",
            "therapy",
            "Therapy",
            "TREAT",
            "Treatment",
            "TX",
            "treatment",
            "treatment or therapy",
            "treatment_or_therapy",
        ],
        "extensions": [
            {
                "name": "associated_with",
                "value": ["umls:C0087111"],
                "type": "Extension"
            }
        ],
        "type": "TherapyDescriptor"
    }


def compare_vod(response, fixture, query, match_type, response_id,
                warnings=None):
    """Verify correctness of returned VOD object against test fixture.

    :param Dict response: actual response
    :param Dict fixture: expected Descriptor object
    :param str query: query used in search
    :param MatchType match_type: expected MatchType
    :param str response_id: expected response_id value
    :param List warnings: expected warnings
    """
    if warnings is None:
        warnings = []

    assert response["query"] == query

    # check warnings
    if warnings:
        assert len(response["warnings"]) == len(warnings), "warnings len"
        for e_warnings in warnings:
            for r_warnings in response["warnings"]:
                for e_key, e_val in e_warnings.items():
                    for _, r_val in r_warnings.items():
                        if e_key == r_val:
                            if isinstance(e_val, list):
                                assert set(r_val) == set(e_val), "warnings val"
                            else:
                                assert r_val == e_val, "warnings val"
    else:
        assert response["warnings"] == [], "warnings != []"

    assert response["match_type"] == match_type

    fixture = fixture.copy()
    fixture["id"] = response_id
    actual = response["therapy_descriptor"]

    assert actual["id"] == fixture["id"]
    assert actual["type"] == fixture["type"]
    assert actual["therapy_id"] == fixture["therapy_id"]
    assert actual["label"] == fixture["label"]

    assert ("xrefs" in actual.keys()) == ("xrefs" in fixture.keys())
    if "xrefs" in actual:
        print(actual["xrefs"])
        print(fixture["xrefs"])
        assert set(actual["xrefs"]) == set(fixture["xrefs"])

    assert ("alternate_labels" in actual.keys()) == ("alternate_labels" in
                                                     fixture.keys())
    if "alternate_labels" in actual:
        assert set(actual["alternate_labels"]) == \
            set(fixture["alternate_labels"])

    def get_extension(extensions, name):
        matches = [e for e in extensions if e["name"] == name]
        if len(matches) > 1:
            assert False
        elif len(matches) == 1:
            return matches[0]
        else:
            return None

    assert ("extensions" in actual.keys()) == ("extensions" in fixture.keys())
    if "extensions" in actual:
        ext_actual = actual["extensions"]
        ext_fixture = fixture["extensions"]

        fda_actual = get_extension(ext_actual, "regulatory_approval")
        fda_fixture = get_extension(ext_fixture, "regulatory_approval")
        assert (fda_actual is None) == (fda_fixture is None), "regulatory_approval"
        if fda_actual and fda_fixture:
            ratings_actual = fda_actual.get("approval_ratings")
            ratings_fixture = fda_fixture.get("approval_ratings")
            if ratings_actual or ratings_fixture:
                assert set(ratings_actual) == set(ratings_fixture)
            assert set(fda_actual.get("approval_year", [])) == \
                set(fda_fixture.get("approval_year", []))
            assert set(fda_actual.get("has_indication", [])) == \
                set(fda_fixture.get("has_indication", []))

        assoc_actual = get_extension(ext_actual, "associated_with")
        assoc_fixture = get_extension(ext_fixture, "associated_with")
        assert (assoc_actual is None) == (assoc_fixture is None)
        if assoc_actual:
            assert assoc_fixture is not None
            assert set(assoc_actual["value"]) == set(assoc_fixture["value"])
            assert assoc_actual["value"]

        tn_actual = get_extension(ext_actual, "trade_names")
        tn_fixture = get_extension(ext_fixture, "trade_names")
        assert (tn_actual is None) == (tn_fixture is None)
        if tn_fixture:
            assert tn_actual is not None
            assert set(tn_actual["value"]) == set(tn_fixture["value"])
            assert tn_actual["value"]

        fda_actual = get_extension(ext_actual, "fda_approval")
        fda_fixture = get_extension(ext_fixture, "fda_approval")
        assert (fda_actual is None) == (fda_fixture is None)
        if fda_fixture:
            assert fda_actual is not None
            assert fda_actual.get("approval_status") == \
                fda_fixture.get("approval_status")
            assert set(fda_actual.get("approval_year", [])) == \
                set(fda_fixture.get("approval_year", []))
            assert set(fda_actual.get("has_indication", [])) == \
                set(fda_fixture.get("has_indication", []))


def test_query(query_handler):
    """Test that query returns properly-structured response."""
    resp = query_handler.search_sources("cisplatin", keyed=False)
    assert resp["query"] == "cisplatin"
    matches = resp["source_matches"]
    assert isinstance(matches, list)
    assert len(matches) == 9
    wikidata = list(filter(lambda m: m["source"] == "Wikidata",
                           matches))[0]
    assert len(wikidata["records"]) == 1
    wikidata_record = wikidata["records"][0]
    assert wikidata_record["label"] == "cisplatin"


def test_query_keyed(query_handler):
    """Test that query structures matches as dict when requested."""
    resp = query_handler.search_sources("penicillin v", keyed=True)
    matches = resp["source_matches"]
    assert isinstance(matches, dict)
    chemidplus = matches["ChemIDplus"]
    chemidplus_record = chemidplus["records"][0]
    assert chemidplus_record["label"] == "Penicillin V"


def test_query_specify_sources(query_handler):
    """Test inclusion and exclusion of sources in query."""
    # test blank params
    resp = query_handler.search_sources("cisplatin", keyed=True)
    assert set(resp["source_matches"].keys()) == {
        "Wikidata", "ChEMBL", "NCIt", "DrugBank", "ChemIDplus", "RxNorm", "HemOnc",
        "GuideToPHARMACOLOGY", "DrugsAtFDA"
    }

    # test partial inclusion
    resp = query_handler.search_sources("cisplatin", keyed=True,
                                        incl="chembl,ncit")
    assert set(resp["source_matches"].keys()) == {"ChEMBL", "NCIt"}

    # test full inclusion
    sources = "chembl,ncit,drugbank,wikidata,rxnorm,chemidplus,hemonc,guidetopharmacology,drugsatfda"  # noqa: E501
    resp = query_handler.search_sources("cisplatin", keyed=True,
                                        incl=sources, excl="")
    assert set(resp["source_matches"].keys()) == {
        "Wikidata", "ChEMBL", "NCIt", "DrugBank", "ChemIDplus", "RxNorm", "HemOnc",
        "GuideToPHARMACOLOGY", "DrugsAtFDA"
    }

    # test partial exclusion
    resp = query_handler.search_sources("cisplatin", keyed=True,
                                        excl="chemidplus")
    assert set(resp["source_matches"].keys()) == {
        "Wikidata", "ChEMBL", "NCIt", "DrugBank", "RxNorm", "HemOnc",
        "GuideToPHARMACOLOGY", "DrugsAtFDA"
    }

    # test full exclusion
    sources = "chembl, wikidata, drugbank, ncit, rxnorm, chemidplus, hemonc, " \
        "guidetopharmacology,drugsatfda"  # noqa: E501
    resp = query_handler.search_sources(
        "cisplatin", keyed=True,
        excl=sources
    )
    assert set(resp["source_matches"].keys()) == set()

    # test case insensitive
    resp = query_handler.search_sources("cisplatin", keyed=True, excl="ChEmBl")
    assert set(resp["source_matches"].keys()) == {
        "Wikidata", "NCIt", "DrugBank", "ChemIDplus", "RxNorm", "HemOnc",
        "GuideToPHARMACOLOGY", "DrugsAtFDA"
    }

    resp = query_handler.search_sources("cisplatin", keyed=True,
                                        incl="wIkIdAtA,cHeMbL")
    assert set(resp["source_matches"].keys()) == {"Wikidata", "ChEMBL"}

    # test error on invalid source names
    with pytest.raises(InvalidParameterException):
        resp = query_handler.search_sources("cisplatin", keyed=True,
                                            incl="chambl")

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterException):
        resp = query_handler.search_sources("cisplatin", keyed=True,
                                            incl="chembl", excl="wikidata")


def test_infer_option(query_handler, merge_query_handler):
    """Test infer_namespace boolean option"""
    # drugbank
    query = "DB01174"
    expected_warnings = [{
        "inferred_namespace": "drugbank",
        "adjusted_query": "drugbank:" + query,
        "alternate_inferred_matches": []
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["DrugBank"]["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    # ncit
    query = "c739"
    expected_warnings = [{
        "inferred_namespace": "ncit",
        "adjusted_query": "ncit:" + query,
        "alternate_inferred_matches": []
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["NCIt"]["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    # chemidplus
    query = "15663-27-1"
    expected_warnings = [{
        "inferred_namespace": "chemidplus",
        "adjusted_query": "chemidplus:" + query,
        "alternate_inferred_matches": [],
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["ChemIDplus"]["match_type"] == \
        MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    # chembl
    query = "chembl11359"
    expected_warnings = [{
        "inferred_namespace": "chembl",
        "adjusted_query": "chembl:" + query,
        "alternate_inferred_matches": [],
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["ChEMBL"]["match_type"] == \
        MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    # wikidata
    query = "q412415"
    expected_warnings = [{
        "inferred_namespace": "wikidata",
        "adjusted_query": "wikidata:" + query,
        "alternate_inferred_matches": [],
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["Wikidata"]["match_type"] == \
        MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    # drugs@fda
    query = "ANDA075036"
    expected_warnings = [{
        "inferred_namespace": "drugsatfda.anda",
        "adjusted_query": "drugsatfda.anda:075036",
        "alternate_inferred_matches": [],
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["DrugsAtFDA"]["match_type"] == \
        MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    query = "nda018057"
    expected_warnings = [{
        "inferred_namespace": "drugsatfda.nda",
        "adjusted_query": "drugsatfda.nda:018057",
        "alternate_inferred_matches": [],
    }]

    response = query_handler.search_sources(query, keyed=True)
    assert response["source_matches"]["DrugsAtFDA"]["match_type"] == \
        MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    response = merge_query_handler.search_groups(query)
    assert response["match_type"] == MatchType.CONCEPT_ID
    assert response["warnings"] == expected_warnings

    # test disabling namespace inference
    query = "DB01174"
    response = merge_query_handler.search_groups(query, infer=False)
    assert response["query"] == query
    assert response["warnings"] == []
    assert "record" not in response
    assert response["match_type"] == MatchType.NO_MATCH


def test_query_merged(merge_query_handler, phenobarbital, cisplatin,
                      spiramycin, therapeutic_procedure):
    """Test that the merged concept endpoint handles queries correctly."""
    # test merged id match
    query = "rxcui:2555"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, cisplatin, query, MatchType.CONCEPT_ID,
                "normalize.therapy:rxcui%3A2555")

    # test concept id match
    query = "chemidplus:50-06-6"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, phenobarbital, query, MatchType.CONCEPT_ID,
                "normalize.therapy:chemidplus%3A50-06-6")

    query = "hemonc:105"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, cisplatin, query, MatchType.CONCEPT_ID,
                "normalize.therapy:hemonc%3A105")

    # test label match
    query = "Phenobarbital"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, phenobarbital, query, MatchType.LABEL,
                "normalize.therapy:Phenobarbital")

    # test trade name match
    query = "Platinol"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, cisplatin, query, MatchType.TRADE_NAME,
                "normalize.therapy:Platinol")

    # test alias match
    query = "cis Diamminedichloroplatinum"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, cisplatin, query, MatchType.ALIAS,
                "normalize.therapy:cis%20Diamminedichloroplatinum")

    query = "Rovamycine"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, spiramycin, query, MatchType.ALIAS,
                "normalize.therapy:Rovamycine")

    # test normalized group with single member
    query = "any therapy"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, therapeutic_procedure, query, MatchType.ALIAS,
                "normalize.therapy:any%20therapy")

    # test that term with multiple possible resolutions resolves at highest
    # match
    query = "Cisplatin"
    response = merge_query_handler.search_groups(query)
    compare_vod(response, cisplatin, query, MatchType.TRADE_NAME,
                "normalize.therapy:Cisplatin")

    # test whitespace stripping
    query = "   Cisplatin "
    response = merge_query_handler.search_groups(query)
    compare_vod(response, cisplatin, query, MatchType.TRADE_NAME,
                "normalize.therapy:Cisplatin")

    # test no match
    query = "zzzz fake therapy zzzz"
    response = merge_query_handler.search_groups(query)
    assert response["query"] == query
    assert response["warnings"] == []
    assert "record" not in response
    assert response["match_type"] == MatchType.NO_MATCH


def test_merged_meta(merge_query_handler):
    """Test population of source and resource metadata in merged querying."""
    query = "phenobarbital"
    response = merge_query_handler.search_groups(query)
    meta_items = response["source_meta_"]
    assert "RxNorm" in meta_items.keys()
    assert "Wikidata" in meta_items.keys()
    assert "NCIt" in meta_items.keys()
    assert "ChemIDplus" in meta_items.keys()

    query = "RP 5337"
    response = merge_query_handler.search_groups(query)
    meta_items = response["source_meta_"]
    assert "NCIt" in meta_items.keys()
    assert "ChemIDplus" in meta_items.keys()


def test_service_meta(query_handler, merge_query_handler):
    """Test service meta info in response."""
    query = "pheno"

    response = query_handler.search_sources(query)
    service_meta = response["service_meta_"]
    assert service_meta["name"] == "thera-py"
    assert service_meta["version"] >= "0.2.13"
    assert isinstance(service_meta["response_datetime"], datetime)
    assert service_meta["url"] == "https://github.com/cancervariants/therapy-normalization"  # noqa: E501

    response = merge_query_handler.search_groups(query)
    service_meta = response["service_meta_"]
    assert service_meta["name"] == "thera-py"
    assert service_meta["version"] >= "0.2.13"
    assert isinstance(service_meta["response_datetime"], datetime)
    assert service_meta["url"] == "https://github.com/cancervariants/therapy-normalization"  # noqa: E501


def test_broken_db_handling(merge_query_handler):
    """Test that query fails gracefully if mission-critical DB references are
    broken.

    The test database includes an identity record (fake:00001) with a
    purposely-broken merge_ref field. This lookup should not raise an
    exception.
    """
    query = "fake:00001"
    assert merge_query_handler.search_groups(query)
