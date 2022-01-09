"""Test the therapy querying method."""
from datetime import datetime
import os
import json
from pathlib import Path

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
def fixture_data(test_data: Path):
    """Fetch fixture data"""
    return json.load(open(test_data / "test_query_data.json", "r"))


@pytest.fixture(scope="module")
def phenobarbital(fixture_data):
    """Create phenobarbital VOD fixture."""
    return fixture_data["phenobarbital"]


@pytest.fixture(scope="module")
def cisplatin(fixture_data):
    """Create cisplatin fixture."""
    return fixture_data["cisplatin"]


@pytest.fixture(scope="module")
def spiramycin(fixture_data):
    """Create fixture for normalized spiramycin record."""
    return fixture_data["spiramycin"]


@pytest.fixture(scope="module")
def therapeutic_procedure(fixture_data):
    """Create a fixture for the Therapeutic Procedure concept. Used to validate
    single-member concept groups for the normalize endpoint.
    """
    return fixture_data["therapeutic_procedure"]


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

    assert bool(actual.get("xrefs")) == bool(fixture.get("xrefs"))
    if actual.get("xrefs"):
        assert set(actual["xrefs"]) == set(fixture["xrefs"])

    assert bool(actual.get("alternate_labels")) == bool(fixture.get("alternate_labels"))
    if actual.get("alternate_labels"):
        assert set(actual["alternate_labels"]) == set(fixture["alternate_labels"])

    def get_extension(extensions, name):
        matches = [e for e in extensions if e["name"] == name]
        if len(matches) > 1:
            assert False
        elif len(matches) == 1:
            return matches[0]
        else:
            return None

    assert bool(actual.get("extensions")) == bool(fixture.get("extensions"))
    if actual.get("extensions"):
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

    # test for not including redundant records w/ same match_type
    resp = query_handler.search_sources("penicillamine", keyed=True)
    matches = resp["source_matches"]["RxNorm"]
    assert len(matches["records"]) == 1


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


def test_query_normalize(merge_query_handler, phenobarbital, cisplatin,
                         spiramycin, therapeutic_procedure):
    """Test that the normalized concept endpoint handles queries correctly."""
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
