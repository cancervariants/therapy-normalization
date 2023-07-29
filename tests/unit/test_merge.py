"""Test merged record generation."""
import os
from pathlib import Path
from typing import Callable, Dict, Set
import random
import json

import pytest

from therapy.database import AWS_ENV_VAR_NAME
from therapy.database.database import create_db
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
def merge_instance(test_source: Callable, is_test_env: bool):
    """Provide fixture for ETL merge class.
    If in a test environment (e.g. CI) this method will attempt to load any missing
    source data, and then perform merged record generation.
    """
    database = create_db()
    if is_test_env:
        if os.environ.get(AWS_ENV_VAR_NAME):
            assert False, (
                f"Running the full disease ETL pipeline test on an AWS environment is "
                f"forbidden -- either unset {AWS_ENV_VAR_NAME} or unset THERAPY_TEST"
            )
        else:
            for SourceClass in (
                ChEMBL, ChemIDplus, DrugBank, DrugsAtFDA, GuideToPHARMACOLOGY, HemOnc,
                NCIt, RxNorm, Wikidata
            ):
                if not database.get_source_metadata(SourceName(SourceClass.__name__)):
                    test_source(SourceClass)

    m = Merge(database)
    if is_test_env:
        concept_ids = database.get_all_concept_ids()
        m.create_merged_concepts(concept_ids)
    return m


def compare_merged_records(actual: Dict, fixture: Dict):
    """Check that records are identical."""
    assert actual["concept_id"] == fixture["concept_id"]
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
        actual_inds_serialized = [i.json() for i in actual_inds]
        actual_inds_serialized.sort()
        fixture_inds_serialized = [json.dumps(i) for i in fixture_inds]
        fixture_inds_serialized.sort()
        for i in range(len(actual_inds_serialized)):
            assert actual_inds_serialized[i] == fixture_inds_serialized[i]


@pytest.fixture(scope="module")
def fixture_data(test_data: Path):
    """Fetch fixture data"""
    return json.load(open(test_data / "fixtures" / "merged_fixtures.json", "r"))


@pytest.fixture(scope="module")
def phenobarbital_merged(fixture_data) -> Dict:
    """Create phenobarbital fixture."""
    return fixture_data["phenobarbital"]


@pytest.fixture(scope="module")
def cisplatin_merged(fixture_data) -> Dict:
    """Create cisplatin fixture."""
    return fixture_data["cisplatin"]


@pytest.fixture(scope="module")
def spiramycin_merged(fixture_data) -> Dict:
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return fixture_data["spiramycin"]


@pytest.fixture(scope="module")
def amifostine_merged(fixture_data) -> Dict:
    """Create fixture for amifostine."""
    return fixture_data["amifostine"]


@pytest.fixture(scope="module")
def record_id_groups():
    """Create fixture for testing concept group creation."""
    return {
        "phenobarbital": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "spiramycin": {
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265"
        },
        "cisplatin": {
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
            "drugsatfda.nda:018057"
        },
        # tests lookup of wikidata reference to rxnorm brand record, and
        # drugbank reference to dead chemidplus record
        "amifostine": {
            "rxcui:1545987",
            "rxcui:4126",
            "ncit:C488",
            "ncit:C66724",
            "hemonc:26",
            "drugbank:DB01143",
            "drugsatfda.nda:020221",
            "chembl:CHEMBL1006",
            "chemidplus:112901-68-5",
            "chemidplus:20537-88-6",
            "wikidata:Q251698"
        },
        "therapeutic procedure": {
            "ncit:C49236"
        },
        # test exclusion of drugs@fda records with multiple UNIIs
        "misc": {
            "drugsatfda.nda:210595"
        }
    }


def test_id_sets(
    merge_instance: Merge,
    record_id_groups: Dict[str, Set[str]]
):
    """Test creation of record ID sets. Queries DB and matches against
    record_id_groups fixture.
    """
    groups_keyed = {}
    for group in record_id_groups.values():
        for concept_id in group:
            groups_keyed[concept_id] = group

    # try a few different orders
    keys = list(groups_keyed.keys())
    key_len = len(keys)
    order0 = list(range(key_len))
    random.seed(42)
    orders = [random.sample(order0, key_len) for _ in range(5)]
    for order in [order0] + orders:
        ordered_keys = [keys[i] for i in order]
        merge_instance._groups = {}
        merge_instance._create_record_id_sets(ordered_keys)  # type: ignore
        groups = merge_instance._groups

        # perform checks
        for concept_id in groups.keys():
            assert groups[concept_id] == groups_keyed[concept_id], concept_id
        assert len(groups) == len(groups_keyed)  # check if any are missing

    # test dead reference
    has_dead_ref = "ncit:C107245"
    dead_group = merge_instance._create_record_id_set(has_dead_ref)
    assert dead_group == {has_dead_ref}


def test_generate_merged_record(
    merge_instance, record_id_groups, phenobarbital_merged, cisplatin_merged,
    spiramycin_merged, amifostine_merged
) -> None:
    """Test generation of individual merged record"""
    merge_instance._groups = {}  # reset from previous tests

    phenobarbital_ids = record_id_groups["phenobarbital"]
    response = merge_instance._generate_merged_record(phenobarbital_ids)
    compare_merged_records(response, phenobarbital_merged)

    cisplatin_ids = record_id_groups["cisplatin"]
    response = merge_instance._generate_merged_record(cisplatin_ids)
    compare_merged_records(response, cisplatin_merged)

    spiramycin_ids = record_id_groups["spiramycin"]
    response = merge_instance._generate_merged_record(spiramycin_ids)
    compare_merged_records(response, spiramycin_merged)

    amifostine_ids = record_id_groups["amifostine"]
    response = merge_instance._generate_merged_record(amifostine_ids)
    compare_merged_records(response, amifostine_merged)
