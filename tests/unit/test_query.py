"""Test the therapy querying method."""

import json
from datetime import datetime
from pathlib import Path

import pytest
from ga4gh.core.models import MappableConcept

from therapy.database.database import AbstractDatabase
from therapy.query import InvalidParameterError, QueryHandler
from therapy.schemas import MatchType, SourceName, Therapy


@pytest.fixture(scope="module")
def handler(database: AbstractDatabase):
    """Build query handler test fixture."""
    return QueryHandler(database)


@pytest.fixture(scope="module")
def search_handler(database: AbstractDatabase):
    """Build query handler test fixture."""

    class QueryGetter:
        def __init__(self):
            self.query_handler = QueryHandler(database)

        def search(self, query_str, incl="", excl="", infer=True):
            return self.query_handler.search(
                query_str=query_str, incl=incl, excl=excl, infer=infer
            )

    return QueryGetter()


@pytest.fixture(scope="module")
def normalize_handler(database: AbstractDatabase):
    """Provide Merge instance to test cases."""

    class QueryGetter:
        def __init__(self):
            self.query_handler = QueryHandler(database)

        def normalize(self, query_str, infer=True):
            return self.query_handler.normalize(query_str, infer)

        def normalize_unmerged(self, query_str, infer=True):
            return self.query_handler.normalize_unmerged(query_str, infer)

    return QueryGetter()


@pytest.fixture(scope="module")
def fixture_data(test_data: Path):
    """Fetch fixture data"""
    with (test_data / "fixtures" / "query_fixtures.json").open() as f:
        return json.load(f)


@pytest.fixture(scope="module")
def normalized_phenobarbital(fixture_data):
    """Create phenobarbital fixture."""
    return fixture_data["normalize_phenobarbital"]


@pytest.fixture(scope="module")
def normalized_cisplatin(fixture_data):
    """Create cisplatin fixture."""
    return fixture_data["normalize_cisplatin"]


@pytest.fixture(scope="module")
def normalized_spiramycin(fixture_data):
    """Create fixture for normalized spiramycin record."""
    return fixture_data["normalize_spiramycin"]


@pytest.fixture(scope="module")
def normalized_therapeutic_procedure(fixture_data):
    """Create a fixture for the Therapeutic Procedure concept. Used to validate
    single-member concept groups for the normalize endpoint.
    """
    return fixture_data["normalize_therapeutic_procedure"]


@pytest.fixture(scope="module")
def unmerged_normalized_cisplatin(fixture_data):
    """Create fixture data for unmerged normalized cisplatin record."""
    return fixture_data["unmerged_normalize_cisplatin"]


@pytest.fixture(scope="module")
def unmerged_normalized_therapeutic_procedure(fixture_data):
    """Create fixture data for unmerged normalized Therapeutic Procedure record.
    Tests whether responses are correctly formed from non-merged normalized result.
    """
    return fixture_data["unmerged_normalize_therapeutic_procedure"]


@pytest.fixture(scope="module")
def unmerged_normalized_spiramycin(fixture_data):
    """Create fixture data for spiramycin. Tests that responses are correctly formed
    from normalized results that only include some (not all) sources.
    """
    return fixture_data["unmerged_normalize_spiramycin"]


def compare_ta(response, fixture, query, match_type, warnings=None):
    """Verify correctness of returned core therapy object against test fixture

    :param Dict response: actual response
    :param Dict fixture: expected therapy object
    :param str query: query used in search
    :param MatchType match_type: expected MatchType
    :param List warnings: expected warnings
    """
    if warnings is None:
        warnings = []

    assert response.query == query

    # check warnings
    if warnings:
        assert len(response.warnings) == len(warnings), "warnings len"
        for e_warnings in warnings:
            for r_warnings in response.warnings:
                for e_key, e_val in e_warnings.items():
                    for r_val in r_warnings.values():
                        if e_key == r_val:
                            if isinstance(e_val, list):
                                assert set(r_val) == set(e_val), "warnings val"
                            else:
                                assert r_val == e_val, "warnings val"
    else:
        assert response.warnings == [], "warnings != []"

    assert response.match_type == match_type

    fixture = MappableConcept(**fixture.copy())
    assert (
        response.therapy.primaryCode.root == fixture.id.split("normalize.therapy.")[-1]
    )
    actual = response.therapy
    actual_keys = actual.model_dump(exclude_none=True).keys()
    fixture_keys = fixture.model_dump(exclude_none=True).keys()
    assert actual_keys == fixture_keys

    assert actual.id == fixture.id
    assert actual.conceptType == fixture.conceptType
    assert actual.label == fixture.label

    assert bool(actual.mappings) == bool(fixture.mappings)
    if actual.mappings:
        no_matches = []
        for actual_mapping in actual.mappings:
            match = None
            for fixture_mapping in fixture.mappings:
                if actual_mapping == fixture_mapping:
                    match = actual_mapping
                    break
            if not match:
                no_matches.append(actual_mapping)
        assert no_matches == [], no_matches
        assert len(actual.mappings) == len(fixture.mappings)

    def get_extension(extensions, name):
        matches = [e for e in extensions if e.name == name]
        if len(matches) > 1:
            pytest.fail(f"Multiple extensions named {name}")
        elif len(matches) == 1:
            return matches[0]
        else:
            return None

    assert bool(actual.extensions) == bool(fixture.extensions)
    if actual.extensions:
        ext_actual = actual.extensions
        ext_fixture = fixture.extensions

        approv_actual = get_extension(ext_actual, "regulatory_approval")
        approv_fixture = get_extension(ext_fixture, "regulatory_approval")
        assert (approv_actual is None) == (
            approv_fixture is None
        ), "regulatory_approval"
        if approv_actual and approv_fixture:
            ratings_actual = approv_actual.value.get("approval_ratings")
            ratings_fixture = approv_fixture.value.get("approval_ratings")
            if ratings_actual or ratings_fixture:
                assert set(ratings_actual) == set(ratings_fixture)
            assert set(approv_actual.value.get("approval_year", [])) == set(
                approv_fixture.value.get("approval_year", [])
            )
            approv_inds = [
                json.dumps(ind, sort_keys=True)
                for ind in approv_actual.value.get("has_indication", [])
            ]
            fixture_inds = [
                json.dumps(ind, sort_keys=True)
                for ind in approv_fixture.value.get("has_indication", [])
            ]
            assert set(approv_inds) == set(fixture_inds)

        tn_actual = get_extension(ext_actual, "trade_names")
        tn_fixture = get_extension(ext_fixture, "trade_names")
        assert (tn_actual is None) == (tn_fixture is None)
        if tn_fixture:
            assert tn_actual
            assert tn_actual.value
            assert set(tn_actual.value) == set(tn_fixture.value)

        fda_actual = get_extension(ext_actual, "fda_approval")
        fda_fixture = get_extension(ext_fixture, "fda_approval")
        assert (fda_actual is None) == (fda_fixture is None)
        if fda_fixture:
            assert fda_actual
            assert fda_actual.value.get("approval_status") == fda_fixture.value.get(
                "approval_status"
            )
            assert set(fda_actual.value.get("approval_year", [])) == set(
                fda_fixture.value.get("approval_year", [])
            )
            assert set(fda_actual.value.get("has_indication", [])) == set(
                fda_fixture.value.get("has_indication", [])
            )


def compare_unmerged_response(
    actual, query, warnings, match_type, fixture, compare_records
):
    """Compare response from normalize unmerged endpoint to fixture."""
    assert actual.query == query
    assert actual.warnings == warnings
    assert actual.match_type == match_type
    assert actual.normalized_concept_id == fixture["normalized_concept_id"]

    for source, match in actual.source_matches.items():
        assert match.source_meta_  # check that it's there
        for record in match.records:
            concept_id = record.concept_id
            fixture_drug = None
            # get corresponding fixture record
            for drug in fixture["source_matches"][source.value]["records"]:
                if drug["concept_id"] == concept_id:
                    fixture_drug = Therapy(**drug)
                    break
            assert fixture_drug, f"Unable to find fixture for {concept_id}"
            compare_records(record, fixture_drug)


def test_search(search_handler):
    """Test that query returns properly-structured response."""
    resp = search_handler.search("cisplatin")
    assert resp.query == "cisplatin"
    matches = resp.source_matches
    assert isinstance(matches, dict)
    assert len(matches) == 9
    wikidata = matches[SourceName.WIKIDATA]
    assert len(wikidata.records) == 1
    wikidata_record = wikidata.records[0]
    assert wikidata_record.label == "cisplatin"

    # test for not including redundant records w/ same match_type
    resp = search_handler.search("penicillamine")
    matches = resp.source_matches[SourceName.RXNORM]
    assert len(matches.records) == 1


def test_search_sources(search_handler):
    """Test inclusion and exclusion of sources in query."""
    # test blank params
    resp = search_handler.search("cisplatin")
    assert set(resp.source_matches.keys()) == {
        "Wikidata",
        "ChEMBL",
        "NCIt",
        "DrugBank",
        "ChemIDplus",
        "RxNorm",
        "HemOnc",
        "GuideToPHARMACOLOGY",
        "DrugsAtFDA",
    }

    # test partial inclusion
    resp = search_handler.search("cisplatin", incl="chembl,ncit")
    assert set(resp.source_matches.keys()) == {"ChEMBL", "NCIt"}

    # test full inclusion
    sources = "chembl,ncit,drugbank,wikidata,rxnorm,chemidplus,hemonc,guidetopharmacology,drugsatfda"
    resp = search_handler.search("cisplatin", incl=sources, excl="")
    assert set(resp.source_matches.keys()) == {
        "Wikidata",
        "ChEMBL",
        "NCIt",
        "DrugBank",
        "ChemIDplus",
        "RxNorm",
        "HemOnc",
        "GuideToPHARMACOLOGY",
        "DrugsAtFDA",
    }

    # test partial exclusion
    resp = search_handler.search("cisplatin", excl="chemidplus")
    assert set(resp.source_matches.keys()) == {
        "Wikidata",
        "ChEMBL",
        "NCIt",
        "DrugBank",
        "RxNorm",
        "HemOnc",
        "GuideToPHARMACOLOGY",
        "DrugsAtFDA",
    }

    # test full exclusion
    sources = "chembl,wikidata,drugbank,ncit,rxnorm,chemidplus,hemonc,guidetopharmacology,drugsatfda"
    resp = search_handler.search("cisplatin", excl=sources)
    assert set(resp.source_matches.keys()) == set()

    # test case insensitive
    resp = search_handler.search("cisplatin", excl="ChEmBl")
    assert set(resp.source_matches.keys()) == {
        "Wikidata",
        "NCIt",
        "DrugBank",
        "ChemIDplus",
        "RxNorm",
        "HemOnc",
        "GuideToPHARMACOLOGY",
        "DrugsAtFDA",
    }

    resp = search_handler.search("cisplatin", incl="wIkIdAtA,cHeMbL")
    assert set(resp.source_matches.keys()) == {"Wikidata", "ChEMBL"}

    # test error on invalid source names
    with pytest.raises(InvalidParameterError):
        resp = search_handler.search("cisplatin", incl="chambl")

    # test error for supplying both incl and excl args
    with pytest.raises(InvalidParameterError):
        resp = search_handler.search("cisplatin", incl="chembl", excl="wikidata")


def test_infer_option(search_handler, normalize_handler):
    """Test infer_namespace boolean option"""
    # drugbank
    query = "DB01174"
    expected_warnings = [
        {
            "inferred_namespace": "drugbank",
            "adjusted_query": "drugbank:" + query,
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert (
        response.source_matches[SourceName.DRUGBANK].match_type == MatchType.CONCEPT_ID
    )
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    # ncit
    query = "c739"
    expected_warnings = [
        {
            "inferred_namespace": "ncit",
            "adjusted_query": "ncit:" + query,
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert response.source_matches["NCIt"].match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    # chemidplus
    query = "15663-27-1"
    expected_warnings = [
        {
            "inferred_namespace": "chemidplus",
            "adjusted_query": "chemidplus:" + query,
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert response.source_matches["ChemIDplus"].match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    # chembl
    query = "chembl11359"
    expected_warnings = [
        {
            "inferred_namespace": "chembl",
            "adjusted_query": "chembl:" + query,
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert response.source_matches["ChEMBL"].match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    # wikidata
    query = "q412415"
    expected_warnings = [
        {
            "inferred_namespace": "wikidata",
            "adjusted_query": "wikidata:" + query,
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert response.source_matches["Wikidata"].match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    # drugs@fda
    query = "ANDA075036"
    expected_warnings = [
        {
            "inferred_namespace": "drugsatfda.anda",
            "adjusted_query": "drugsatfda.anda:075036",
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert response.source_matches["DrugsAtFDA"].match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    query = "nda018057"
    expected_warnings = [
        {
            "inferred_namespace": "drugsatfda.nda",
            "adjusted_query": "drugsatfda.nda:018057",
            "alternate_inferred_matches": [],
        }
    ]

    response = search_handler.search(query)
    assert response.source_matches["DrugsAtFDA"].match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    response = normalize_handler.normalize(query)
    assert response.match_type == MatchType.CONCEPT_ID
    assert response.warnings == expected_warnings

    # test disabling namespace inference
    query = "DB01174"
    response = normalize_handler.normalize(query, infer=False)
    assert response.query == query
    assert response.warnings == []
    assert "record" not in response
    assert response.match_type == MatchType.NO_MATCH


def test_query_normalize(
    normalize_handler,
    normalized_phenobarbital,
    normalized_cisplatin,
    normalized_spiramycin,
    normalized_therapeutic_procedure,
):
    """Test that the normalized concept endpoint handles queries correctly."""
    # test merged id match
    query = "rxcui:2555"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_cisplatin, query, MatchType.CONCEPT_ID)

    # test concept id match
    query = "chemidplus:50-06-6"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_phenobarbital, query, MatchType.CONCEPT_ID)

    query = "hemonc:105"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_cisplatin, query, MatchType.CONCEPT_ID)

    # test label match
    query = "Phenobarbital"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_phenobarbital, query, MatchType.LABEL)

    # test trade name match
    query = "Platinol"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_cisplatin, query, MatchType.TRADE_NAME)

    # test alias match
    query = "cis-ddp"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_cisplatin, query, MatchType.ALIAS)

    query = "Rovamycine"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_spiramycin, query, MatchType.ALIAS)

    # test normalized group with single member
    query = "any therapy"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_therapeutic_procedure, query, MatchType.ALIAS)

    # test that term with multiple possible resolutions resolves at highest
    # match
    query = "Cisplatin"
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_cisplatin, query, MatchType.TRADE_NAME)

    # test whitespace stripping
    query = "   Cisplatin "
    response = normalize_handler.normalize(query)
    compare_ta(response, normalized_cisplatin, query, MatchType.TRADE_NAME)

    # test no match
    query = "zzzz fake therapy zzzz"
    response = normalize_handler.normalize(query)
    assert response.query == query
    assert response.warnings == []
    assert "record" not in response
    assert response.match_type == MatchType.NO_MATCH


def test_unmerged_normalize(
    normalize_handler,
    compare_records,
    unmerged_normalized_spiramycin,
    unmerged_normalized_cisplatin,
    unmerged_normalized_therapeutic_procedure,
):
    """Test correctness of unmerged normalize endpoint."""
    # concept ID match
    query = "rxcui:2555"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.CONCEPT_ID,
        unmerged_normalized_cisplatin,
        compare_records,
    )

    query = "chembl:CHEMBL11359"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.CONCEPT_ID,
        unmerged_normalized_cisplatin,
        compare_records,
    )
    query = "ncit:C49236"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.CONCEPT_ID,
        unmerged_normalized_therapeutic_procedure,
        compare_records,
    )

    query = "rxcui:9991"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.CONCEPT_ID,
        unmerged_normalized_spiramycin,
        compare_records,
    )

    query = "ncit:c839"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.CONCEPT_ID,
        unmerged_normalized_spiramycin,
        compare_records,
    )

    # namespace infer
    query = "CHEMBL11359"
    response = normalize_handler.normalize_unmerged(query)
    warning = {
        "inferred_namespace": "chembl",
        "adjusted_query": "chembl:CHEMBL11359",
        "alternate_inferred_matches": [],
    }
    compare_unmerged_response(
        response,
        query,
        [warning],
        MatchType.CONCEPT_ID,
        unmerged_normalized_cisplatin,
        compare_records,
    )

    # label match
    query = "cisplatin"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.LABEL,
        unmerged_normalized_cisplatin,
        compare_records,
    )

    query = "therapeutic procedure"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.LABEL,
        unmerged_normalized_therapeutic_procedure,
        compare_records,
    )

    query = "spiramycin"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.LABEL,
        unmerged_normalized_spiramycin,
        compare_records,
    )

    # alias/xref/trade name/associated with
    query = "Platinol-aq"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.TRADE_NAME,
        unmerged_normalized_cisplatin,
        compare_records,
    )

    query = "ndc:0143-9504"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.ASSOCIATED_WITH,
        unmerged_normalized_cisplatin,
        compare_records,
    )

    query = "CIS-DDP"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.ALIAS,
        unmerged_normalized_cisplatin,
        compare_records,
    )

    query = "TREAT"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.ALIAS,
        unmerged_normalized_therapeutic_procedure,
        compare_records,
    )

    query = "umls:C0087111"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.ASSOCIATED_WITH,
        unmerged_normalized_therapeutic_procedure,
        compare_records,
    )

    query = "rovamycin"
    response = normalize_handler.normalize_unmerged(query)
    compare_unmerged_response(
        response,
        query,
        [],
        MatchType.ALIAS,
        unmerged_normalized_spiramycin,
        compare_records,
    )


def test_merged_meta(normalize_handler):
    """Test population of source and resource metadata in merged querying."""
    query = "phenobarbital"
    response = normalize_handler.normalize(query)
    meta_items = response.source_meta_
    assert SourceName.RXNORM in meta_items
    assert SourceName.WIKIDATA in meta_items
    assert SourceName.NCIT in meta_items
    assert SourceName.CHEMIDPLUS in meta_items

    query = "RP 5337"
    response = normalize_handler.normalize(query)
    meta_items = response.source_meta_
    assert SourceName.NCIT in meta_items
    assert SourceName.CHEMIDPLUS in meta_items


def test_service_meta(search_handler, normalize_handler):
    """Test service meta info in response."""
    query = "pheno"

    response = search_handler.search(query)
    service_meta = response.service_meta_
    assert service_meta.name == "thera-py"
    assert service_meta.version != "unknown"
    assert isinstance(service_meta.response_datetime, datetime)
    assert service_meta.url == "https://github.com/cancervariants/therapy-normalization"

    response = normalize_handler.normalize(query)
    service_meta = response.service_meta_
    assert service_meta.name == "thera-py"
    assert service_meta.version != "unknown"
    assert isinstance(service_meta.response_datetime, datetime)
    assert service_meta.url == "https://github.com/cancervariants/therapy-normalization"


def test_broken_db_handling(normalize_handler):
    """Test that query fails gracefully if mission-critical DB references are
    broken.

    The test database includes an identity record (fake:00001) with a
    purposely-broken merge_ref field. This lookup should not raise an
    exception.
    """
    query = "fake:00001"
    assert normalize_handler.normalize(query)
