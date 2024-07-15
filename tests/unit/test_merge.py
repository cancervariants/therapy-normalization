"""Test merged record generation."""

import json
import os
import random
from pathlib import Path

import pytest

from therapy.database import AWS_ENV_VAR_NAME
from therapy.etl.chembl import ChEMBL
from therapy.etl.chemidplus import ChemIDplus
from therapy.etl.drugbank import DrugBank
from therapy.etl.drugsatfda import DrugsAtFDA
from therapy.etl.guidetopharmacology import GuideToPHARMACOLOGY
from therapy.etl.hemonc import HemOnc
from therapy.etl.merge import Merge
from therapy.etl.ncit import NCIt
from therapy.etl.rxnorm import RxNorm
from therapy.etl.wikidata import Wikidata
from therapy.schemas import SourceName


@pytest.fixture(scope="module")
def merge_instance(test_source, is_test_env, database):
    """Provide fixture for ETL merge class"""
    if is_test_env:
        if os.environ.get(AWS_ENV_VAR_NAME):
            pytest.fail(
                f"Running the full therapy ETL pipeline test on an AWS environment is forbidden -- either unset {AWS_ENV_VAR_NAME} or unset THERAPY_TEST"
            )
        for SourceClass in (  # noqa: N806
            ChEMBL,
            ChemIDplus,
            DrugBank,
            DrugsAtFDA,
            GuideToPHARMACOLOGY,
            HemOnc,
            NCIt,
            RxNorm,
            Wikidata,
        ):
            test_source(SourceClass)
    return Merge(database)


def compare_merged_records(actual: dict, fixture: dict):
    """Check that records are identical."""
    assert actual["concept_id"] == fixture["concept_id"]
    assert actual["label_and_type"] == fixture["label_and_type"]
    assert ("label" in actual) == ("label" in fixture)
    if "label" in actual or "label" in fixture:
        assert actual["label"] == fixture["label"]
    assert ("trade_names" in actual) == ("trade_names" in fixture)
    if "trade_names" in actual or "trade_names" in fixture:
        assert set(actual["trade_names"]) == set(fixture["trade_names"])
    assert ("aliases" in actual) == ("aliases" in fixture)
    if "aliases" in actual or "aliases" in fixture:
        assert set(actual["aliases"]) == set(fixture["aliases"])
    assert ("xrefs" in actual) == ("xrefs" in fixture)
    if "xrefs" in actual or "xrefs" in fixture:
        assert set(actual["xrefs"]) == set(fixture["xrefs"])
    assert ("associated_with" in actual) == ("associated_with" in fixture)
    if "associated_with" in actual or "associated_with" in fixture:
        assert set(actual["associated_with"]) == set(fixture["associated_with"])
    assert ("approval_ratings" in actual) == ("approval_ratings" in fixture)
    if "approval_ratings" in actual or "approval_ratings" in fixture:
        assert set(actual["approval_ratings"]) == set(fixture["approval_ratings"])
    assert ("approval_year" in actual) == ("approval_year" in fixture)
    if "approval_year" in actual or "approval_year" in fixture:
        assert set(actual["approval_year"]) == set(fixture["approval_year"])
    assert ("has_indication" in actual) == ("has_indication" in fixture)
    if "has_indication" in actual or "has_indication" in fixture:
        actual_inds = actual["has_indication"].copy()
        fixture_inds = fixture["has_indication"].copy()
        assert len(actual_inds) == len(fixture_inds)
        actual_inds.sort()
        fixture_inds.sort()
        for i in range(len(actual_inds)):
            assert actual_inds[i] == fixture_inds[i]


@pytest.fixture(scope="module")
def fixture_data(test_data: Path):
    """Fetch fixture data"""
    with (test_data / "fixtures" / "merged_fixtures.json").open() as f:
        return json.load(f)


@pytest.fixture(scope="module")
def phenobarbital_merged(fixture_data) -> dict:
    """Create phenobarbital fixture."""
    return fixture_data["phenobarbital"]


@pytest.fixture(scope="module")
def cisplatin_merged(fixture_data) -> dict:
    """Create cisplatin fixture."""
    return fixture_data["cisplatin"]


@pytest.fixture(scope="module")
def spiramycin_merged(fixture_data) -> dict:
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return fixture_data["spiramycin"]


@pytest.fixture(scope="module")
def amifostine_merged(fixture_data) -> dict:
    """Create fixture for amifostine."""
    return fixture_data["amifostine"]


@pytest.fixture(scope="module")
def record_id_groups():
    """Create fixture for testing concept group creation."""
    groups = [
        # phenobarbital
        {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804",
        },
        # spiramycin
        {
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265",
        },
        # cisplatin
        {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda.anda:074656",
            "drugsatfda.anda:074735",
            "drugsatfda.anda:206774",
            "drugsatfda.anda:207323",
            "drugsatfda.anda:075036",
            "iuphar.ligand:5343",
            "drugsatfda.nda:018057",
        },
        # Amifostine
        # tests lookup of wikidata reference to rxnorm brand record, and
        # drugbank reference to dead chemidplus record
        {
            "rxcui:1545987",
            "rxcui:4126",
            "ncit:C488",
            "ncit:C66724",
            "drugbank:DB01143",
            "drugsatfda.nda:020221",
            "chembl:CHEMBL1006",
            "chemidplus:112901-68-5",
            "chemidplus:20537-88-6",
            "wikidata:Q251698",
        },
        # Therapeutic Procedure
        {"ncit:C49236"},
        # test exclusion of drugs@fda records with multiple UNIIs
        {"drugsatfda.nda:210595"},
    ]
    groups_keyed = {}
    for group in groups:
        for concept_id in group:
            groups_keyed[concept_id] = group
    return groups_keyed


def test_id_sets(merge_instance: Merge, record_id_groups: dict[str, set[str]]):
    """Test creation of record ID sets. Queries DB and matches against
    record_id_groups fixture.
    """
    # try a few different orders
    keys = list(record_id_groups)
    key_len = len(keys)
    order0 = list(range(key_len))
    random.seed(42)
    orders = [random.sample(order0, key_len) for _ in range(5)]
    for order in [order0, *orders]:
        ordered_keys = [keys[i] for i in order]
        merge_instance._create_record_id_sets(ordered_keys)  # type: ignore
        groups = merge_instance._groups

        # perform checks
        for concept_id in groups:
            assert groups[concept_id] == record_id_groups[concept_id], f"{concept_id}"
        assert len(groups) == len(record_id_groups)  # check if any are missing

    # test dead reference
    has_dead_ref = "ncit:C107245"
    dead_group = merge_instance._create_record_id_set(has_dead_ref)
    assert dead_group == {has_dead_ref}


def test_generate_merged_record(
    merge_instance: Merge,
    record_id_groups: dict[str, set[str]],
    phenobarbital_merged: dict,
    cisplatin_merged: dict,
    spiramycin_merged: dict,
):
    """Test generation of merged record method."""
    phenobarbital_ids = record_id_groups["rxcui:8134"]
    merge_response = merge_instance._generate_merged_record(phenobarbital_ids)
    compare_merged_records(merge_response, phenobarbital_merged)

    cisplatin_ids = record_id_groups["rxcui:2555"]
    merge_response = merge_instance._generate_merged_record(cisplatin_ids)
    compare_merged_records(merge_response, cisplatin_merged)

    spiramycin_ids = record_id_groups["ncit:C839"]
    merge_response = merge_instance._generate_merged_record(spiramycin_ids)
    compare_merged_records(merge_response, spiramycin_merged)


def test_create_merged_concepts(
    merge_instance: Merge,
    record_id_groups: dict[str, set[str]],
    phenobarbital_merged: dict,
    cisplatin_merged: dict,
    spiramycin_merged: dict,
    amifostine_merged: dict,
    mocker,
):
    """Test end-to-end creation and upload of merged concepts."""
    add_spy = mocker.spy(merge_instance.database, "add_merged_record")
    update_spy = mocker.spy(merge_instance.database, "update_merge_ref")
    merge_instance.create_merged_concepts(set(record_id_groups))
    merge_instance.database.complete_write_transaction()

    # check merged record generation and storage
    added_records = {
        call[1][0]["concept_id"]: call[1][0] for call in add_spy.mock_calls
    }

    phenobarbital_merged_id = phenobarbital_merged["concept_id"]
    assert phenobarbital_merged_id in added_records
    compare_merged_records(added_records[phenobarbital_merged_id], phenobarbital_merged)

    cisplatin_merged_id = cisplatin_merged["concept_id"]
    assert cisplatin_merged_id in added_records
    compare_merged_records(added_records[cisplatin_merged_id], cisplatin_merged)

    spiramycin_merged_id = spiramycin_merged["concept_id"]
    assert spiramycin_merged_id in added_records
    compare_merged_records(added_records[spiramycin_merged_id], spiramycin_merged)

    amifostine_merged_id = amifostine_merged["concept_id"]
    assert amifostine_merged_id in added_records
    compare_merged_records(added_records[amifostine_merged_id], amifostine_merged)

    # should only create new records for groups with n > 1 members
    assert add_spy.call_count == 4

    # check merged record reference updating
    updated_records = {k[1][0]: k[1][1] for k in update_spy.mock_calls}
    for concept_id in record_id_groups["rxcui:8134"]:
        assert updated_records[concept_id] == phenobarbital_merged["concept_id"].lower()
    for concept_id in record_id_groups["rxcui:2555"]:
        assert updated_records[concept_id] == cisplatin_merged["concept_id"].lower()
    for concept_id in record_id_groups["ncit:C839"]:
        assert updated_records[concept_id] == spiramycin_merged["concept_id"].lower()

    # no merged record should be generated
    assert "ncit:C49236" not in updated_records
    assert "drugsatfda.nda:210595" not in updated_records

    assert update_spy.call_count == len(record_id_groups) - 2


def test_merge_record_ordering(merge_instance: Merge):
    """Test record ordering within merged record generation.

    This test uses dummy data and only checks the ordering pipeline because relevant
    edge cases are quite large and would entail considerable effort to maintain fixtures.
    """
    records = [
        {
            "label": "Trastuzumab",
            "src_name": SourceName.HEMONC,
            "concept_id": "hemonc:1",
        },
        {
            "label": "trastuzumab-xxxx",
            "src_name": SourceName.RXNORM,
            "concept_id": "rxcui:1",
        },
        {
            "label": "Trastuzumab",
            "src_name": SourceName.RXNORM,
            "concept_id": "rxcui:2",
        },
    ]
    result = merge_instance._sort_records(records)
    assert [r["concept_id"] for r in result] == ["rxcui:2", "rxcui:1", "hemonc:1"]

    records = [
        {
            "label": "drug",
            "src_name": SourceName.DRUGSATFDA,
            "concept_id": "drugsatfda:1",
        },
        {"label": "drug", "src_name": SourceName.NCIT, "concept_id": "ncit:1"},
        {
            "label": "Trastuzumab-xxxx",
            "src_name": SourceName.RXNORM,
            "concept_id": "rxcui:1",
        },
        {
            "label": "Trastuzumab-yyyy",
            "src_name": SourceName.RXNORM,
            "concept_id": "rxcui:2",
        },
    ]
    result = merge_instance._sort_records(records)
    assert [r["concept_id"] for r in result] == [
        "rxcui:1",
        "rxcui:2",
        "ncit:1",
        "drugsatfda:1",
    ]

    records = [
        {
            "label": "trastuzumab-aaaa",
            "src_name": SourceName.RXNORM,
            "concept_id": "rxcui:2",
        },
        {
            "label": "trastuzumab",
            "src_name": SourceName.CHEMBL,
            "concept_id": "chembl:CHEMBL1",
        },
    ]
    result = merge_instance._sort_records(records)
    assert [r["concept_id"] for r in result] == ["rxcui:2", "chembl:CHEMBL1"]
