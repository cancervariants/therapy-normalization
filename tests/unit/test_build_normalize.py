"""Test merged record generation."""
from typing import Dict, List
import json
from pathlib import Path

import pytest

from therapy.etl.normalize import NormalizeBuilder


class MockNormalizeBuilder(NormalizeBuilder):
    """Mock version of NormalizeBuilder."""

    def __init__(self, database):
        """Initialize instance.
        :param MockDatabase database: mock database to use
        """
        super().__init__(database)

    def get_added_records(self) -> List:
        """Fetch records that have been added.
        :return: List of added records
        """
        return self.database.added_records  # type: ignore

    def get_updates(self) -> List:
        """Fetch update commands that have been issued.
        :return: List of update items
        """
        return self.database.updates  # type: ignore

    def refresh_changes(self):
        """Clear changes to set up for another test run."""
        self.database.updates = {}  # type: ignore
        self.database.added_records = {}  # type: ignore
        self._groups = {}


@pytest.fixture(scope="module")
def normalize_builder(mock_database):
    """Provide Merge instance to test cases."""
    return MockNormalizeBuilder(mock_database)


def compare_merged_records(actual: Dict, fixture: Dict):
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
        actual_inds.sort(key=lambda x: x[0])
        fixture_inds.sort(key=lambda x: x[0])
        for i in range(len(actual_inds)):
            assert actual_inds[i] == fixture_inds[i]


@pytest.fixture(scope="module")
def fixture_data(test_data: Path):
    """Fetch fixture data"""
    return json.load(open(test_data / "test_merge_data.json", "r"))


@pytest.fixture(scope="module")
def phenobarbital_merged(fixture_data):
    """Create phenobarbital fixture."""
    return fixture_data["phenobarbital"]


@pytest.fixture(scope="module")
def cisplatin_merged(fixture_data):
    """Create cisplatin fixture."""
    return fixture_data["cisplatin"]


@pytest.fixture(scope="module")
def spiramycin_merged(fixture_data):
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return fixture_data["spiramycin"]


@pytest.fixture(scope="module")
def record_id_groups():
    """Create fixture for concept group sets."""
    return {
        "rxcui:8134": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "ncit:C739": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "chemidplus:50-06-6": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "wikidata:Q407241": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "drugbank:DB01174": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "chembl:CHEMBL40": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "iuphar.ligand:2804": {  # Phenobarbital
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        },
        "rxcui:9991": {  # Spiramycin
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265"
        },
        "ncit:C839": {  # Spiramycin
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265"
        },
        "chemidplus:8025-81-8": {  # Spiramycin
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265"
        },
        "drugbank:DB06145": {  # Spiramycin
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265"
        },
        "wikidata:Q422265": {  # Spiramycin
            "rxcui:9991",
            "ncit:C839",
            "chemidplus:8025-81-8",
            "drugbank:DB06145",
            "wikidata:Q422265"
        },
        "rxcui:2555": {  # Cisplatin
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
            "drugsatfda.nda:018057",
            "iuphar.ligand:5343"
        },
        "ncit:C376": {  # Cisplatin
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
        "chemidplus:15663-27-1": {  # Cisplatin
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
        "wikidata:Q412415": {  # Cisplatin
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
        "drugbank:DB00515": {  # Cisplatin
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
        "hemonc:105": {  # Cisplatin
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
        "chembl:CHEMBL11359": {  # Cisplatin
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
        "drugsatfda.anda:074656": {  # Cisplatin
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
        "drugsatfda.anda:074735": {  # Cisplatin
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
        "drugsatfda.anda:206774": {  # Cisplatin
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
        "drugsatfda.anda:207323": {  # Cisplatin
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
        "drugsatfda.anda:075036": {  # Cisplatin
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
        "drugbank:DB12117": {  # Cisplatin
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
        "drugsatfda.nda:018057": {  # Cisplatin
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
        "iuphar.ligand:5343": {  # Cisplatin
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
        "rxcui:4126": {  # Amifostine
            "rxcui:4126",
            "wikidata:Q47521576",
            "drugbank:DB01143"
        },
        "wikidata:Q47521576": {  # Amifostine
            "rxcui:4126",
            "wikidata:Q47521576",
            "drugbank:DB01143"
        },
        "drugbank:DB01143": {  # Amifostine
            "rxcui:4126",
            "wikidata:Q47521576",
            "drugbank:DB01143"
        },
        "ncit:C49236": {  # Therapeutic Procedure
            "ncit:C49236"
        },
        # test exclusion of drugs@fda records with multiple UNIIs
        "drugsatfda.nda:210595": {
            "drugsatfda.nda:210595"
        }
    }


def test_create_record_id_set(normalize_builder: MockNormalizeBuilder,
                              # record_id_groups
                              ):
    """Test creation of record ID sets. Queries DB and matches against
    record_id_groups fixture.
    """
    normalize_builder._create_normalized_groups()
    breakpoint()
    # try a few different orders
    # random.seed(42)
    # for _ in range(10):
    #     records = normalize_builder.records
    #     for key in records:
    #         to_shuffle = list(records[key].items())
    #         random.shuffle(to_shuffle)
    #         records[key] = dict(to_shuffle)
    #     to_shuffle = list(records.items())  # type: ignore
    #     random.shuffle(to_shuffle)
    #     normalize_builder.records = to_shuffle

    #     normalize_builder.
    # keys = list(record_id_groups.keys())
    # key_len = len(keys)
    # order0 = list(range(key_len))
    # random.seed(42)
    # orders = [random.sample(order0, key_len) for _ in range(5)]
    # for order in [order0] + orders:
    #     ordered_keys = [keys[i] for i in order]
    #     merge_handler.merge._groups = {}

    #     for record_id in ordered_keys:
    #         new_group = merge_handler.create_record_id_set(record_id)
    #         if new_group:
    #             for concept_id in new_group:
    #                 merge_handler.merge._groups[concept_id] = new_group
    #     groups = merge_handler.merge._groups

    #     # perform checks
    #     for concept_id in groups.keys():
    #         assert groups[concept_id] == record_id_groups[concept_id],
    # f"{concept_id}"
    #     assert len(groups) == len(record_id_groups)
    # # check if any are missing

    # # test dead reference
    # has_dead_ref = "ncit:C107245"
    # dead_group = merge_handler.create_record_id_set(has_dead_ref)
    # assert dead_group == {has_dead_ref}


def test_generate_merged_record(merge_handler, record_id_groups,
                                phenobarbital_merged, cisplatin_merged,
                                spiramycin_merged):
    """Test generation of merged record method."""
    phenobarbital_ids = record_id_groups["rxcui:8134"]
    merge_response = merge_handler.generate_merged_record(phenobarbital_ids)
    compare_merged_records(merge_response, phenobarbital_merged)

    cisplatin_ids = record_id_groups["rxcui:2555"]
    merge_response = merge_handler.generate_merged_record(cisplatin_ids)
    compare_merged_records(merge_response, cisplatin_merged)

    spiramycin_ids = record_id_groups["ncit:C839"]
    merge_response = merge_handler.generate_merged_record(spiramycin_ids)
    compare_merged_records(merge_response, spiramycin_merged)


def test_create_merged_concepts(merge_handler, record_id_groups,
                                phenobarbital_merged, cisplatin_merged,
                                spiramycin_merged):
    """Test end-to-end creation and upload of merged concepts."""
    record_ids = record_id_groups.keys()
    merge_handler.create_merged_concepts(record_ids)

    # check merged record generation and storage
    added_records = merge_handler.get_added_records()
    assert len(added_records) == 4

    phenobarb_merged_id = phenobarbital_merged["concept_id"]
    assert phenobarb_merged_id in added_records.keys()
    compare_merged_records(added_records[phenobarb_merged_id],
                           phenobarbital_merged)

    cispl_merged_id = cisplatin_merged["concept_id"]
    assert cispl_merged_id in added_records.keys()
    compare_merged_records(added_records[cispl_merged_id], cisplatin_merged)

    spira_merged_id = spiramycin_merged["concept_id"]
    assert spira_merged_id in added_records.keys()
    compare_merged_records(added_records[spira_merged_id],
                           spiramycin_merged)

    # check merged record reference updating
    updates = merge_handler.get_updates()
    for concept_id in record_id_groups["rxcui:8134"]:
        assert updates[concept_id] == {
            "merge_ref": phenobarbital_merged["concept_id"].lower()
        }
    for concept_id in record_id_groups["rxcui:2555"]:
        assert updates[concept_id] == {
            "merge_ref": cisplatin_merged["concept_id"].lower()
        }
    for concept_id in record_id_groups["ncit:C839"]:
        assert updates[concept_id] == {
            "merge_ref": spiramycin_merged["concept_id"].lower()
        }

    # no merged record for ncit:C49236 should be generated
    assert len(updates) == len(record_id_groups) - 2
    assert "ncit:C49236" not in updates
    assert "drugsatfda.nda:210595" not in updates
