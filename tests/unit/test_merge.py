"""Test merged record generation."""
from typing import Dict
import random

import pytest

from therapy.etl.merge import Merge


@pytest.fixture(scope="module")
def merge_handler(mock_database):
    """Provide Merge instance to test cases.
    Implements interfaces for basic merge functions, injects mock DB and
    enables some additional backend checks for correctness.
    """
    class MergeHandler:
        def __init__(self):
            self.merge = Merge(mock_database())

        def get_merge(self):
            return self.merge

        def create_merged_concepts(self, record_ids):
            return self.merge.create_merged_concepts(record_ids)

        def get_added_records(self):
            return self.merge.database.added_records  # type: ignore

        def get_updates(self):
            return self.merge.database.updates  # type: ignore

        def create_record_id_set(self, record_id):
            return self.merge._create_record_id_set(record_id)

        def generate_merged_record(self, record_id_set):
            return self.merge._generate_merged_record(record_id_set)

    return MergeHandler()


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
        assert set(actual["associated_with"]) == \
            set(fixture["associated_with"])

    assert ("approval_ratings" in actual) == ("approval_ratings" in fixture)
    if "approval_ratings" in actual or "approval_ratings" in fixture:
        assert set(actual["approval_ratings"]) == \
            set(fixture["approval_ratings"])
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
def phenobarbital_merged():
    """Create phenobarbital fixture."""
    return {
        "label_and_type": "rxcui:8134##merger",
        "concept_id": "rxcui:8134",
        "xrefs": [
            "ncit:C739",
            "drugbank:DB01174",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "chembl:CHEMBL40",
            "iuphar.ligand:2804"
        ],
        "aliases": [
            "5-Ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione",
            "5-Ethyl-5-phenyl-pyrimidine-2,4,6-trione",
            "5-Ethyl-5-phenylbarbituric acid",
            "5-Phenyl-5-ethylbarbituric acid",
            "5-ethyl-5-phenyl-1,3-diazinane-2,4,6-trione",
            "5-ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione",
            "5-ethyl-5-phenylpyrimidine-2,4,6(1H,3H,5H)-trione",
            "APRD00184",
            "Acid, Phenylethylbarbituric",
            "Eskabarb",
            "Fenobarbital",
            "Luminal",
            "Luminal®",
            "NSC-128143",
            "NSC-128143-",
            "NSC-9848",
            "Noptil",
            "PHENO",
            "PHENOBARBITAL",
            "PHENYLETHYLMALONYLUREA",
            "PHENobarbital",
            "Phenemal",
            "Phenobarbital",
            "Phenobarbital civ",
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
            "Solfoton",
            "Talpheno",
            "fenobarbital",
            "phenobarb",
            "phenobarbital",
            "phenobarbital sodium",
            "phenobarbitone",
            "phenylethylbarbiturate"
        ],
        "associated_with": [
            "pubchem.compound:4763",
            "usp:m63400",
            "gsddb:2179",
            "snomedct:51073002",
            "vandf:4017422",
            "mmsl:2390",
            "msh:D010634",
            "snomedct:373505007",
            "mmsl:5272",
            "unii:YQE403BP4D",
            "fdbmk:001406",
            "mmsl:d00340",
            "atc:N03AA02",
            "umls:C0031412",
            "CHEBI:8069",
            "inchikey:DDBREPKUVSBGFI-UHFFFAOYSA-N",
            "drugcentral:2134",
            "pubchem.substance:135650817"
        ],
        "label": "Phenobarbital",
        "approval_ratings": [
            "rxnorm_prescribable",
            "chembl_phase_4",
            "gtopdb_approved"
        ]
    }


@pytest.fixture(scope="module")
def cisplatin_merged():
    """Create cisplatin fixture."""
    return {
        "label_and_type": "rxcui:2555##merger",
        "concept_id": "rxcui:2555",
        "xrefs": [
            "ncit:C376",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        ],
        "trade_names": [
            "Cisplatin",
            "Platinol",
            "PLATINOL",
            "PLATINOL-AQ"
        ],
        "aliases": [
            "1,2-Diaminocyclohexaneplatinum II citrate",
            "APRD00359",
            "CDDP",
            "CISplatin",
            "Cisplatin",
            "Cisplatinum",
            "Cis-Platinum II",
            "Cis-DDP",
            "CIS-DDP",
            "DACP",
            "DDP",
            "Diamminodichloride, Platinum",
            "Dichlorodiammineplatinum",
            "INT-230-6 COMPONENT CISPLATIN",
            "INT230-6 COMPONENT CISPLATIN",
            "Platinum Diamminodichloride",
            "cis Diamminedichloroplatinum",
            "cis Platinum",
            "cis-Diaminedichloroplatinum",
            "cis-Diamminedichloroplatinum",
            "cis-diamminedichloroplatinum(II)",
            "cis-Diamminedichloroplatinum(II)",
            "cis-Dichlorodiammineplatinum(II)",
            "cisplatinum",
            "cis-Platinum",
            "cis-platinum",
            "cisplatino",
            "cis-diamminedichloroplatinum(II)",
            "cis-diamminedichloroplatinum III",
            "NSC 119875",
            "NSC-119875",
            "Platinol-AQ",
            "Platinol-Aq",
            "Platinol"
        ],
        "label": "cisplatin",
        "associated_with": [
            "mmsl:31747",
            "mmsl:4456",
            "mmsl:d00195",
            "usp:m17910",
            "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L",
            "inchikey:MOTIYCLHZZLHHQ-UHFFFAOYSA-N",
            "mesh:D002945",
            "atc:L01XA01",
            "vandf:4018139",
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
        ],
        "approval_ratings": [
            "rxnorm_prescribable",
            "hemonc_approved",
            "fda_prescription",
            "chembl_phase_4"
        ],
        "approval_year": ["1978"],
        "has_indication": [
            ["hemonc:671", "Testicular cancer", "ncit:C7251"],
            ["hemonc:645", "Ovarian cancer", "ncit:C7431"],
            ["hemonc:569", "Bladder cancer", "ncit:C9334"]
        ],
    }


@pytest.fixture(scope="module")
def spiramycin_merged():
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return {
        "label_and_type": "ncit:c839##merger",
        "concept_id": "ncit:C839",
        "xrefs": [
            "chemidplus:8025-81-8",
        ],
        "label": "Spiramycin",
        "aliases": [
            "SPIRAMYCIN",
            "Antibiotic 799",
            "Rovamycin",
            "Provamycin",
            "Rovamycine",
            "RP 5337",
            "(4R,5S,6R,7R,9R,10R,11E,13E,16R)-10-{[(2R,5S,6R)-5-(dimethylamino)-6-methyltetrahydro-2H-pyran-2-yl]oxy}-9,16-dimethyl-5-methoxy-2-oxo-7-(2-oxoethyl)oxacyclohexadeca-11,13-dien-6-yl 3,6-dideoxy-4-O-(2,6-dideoxy-3-C-methyl-alpha-L-ribo-hexopyranosyl)-3-(dimethylamino)-alpha-D-glucopyranoside"  # noqa: E501
        ],
        "associated_with": [
            "umls:C0037962",
            "unii:71ODY0V87H"
        ],
    }


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
        "ncit:C839": {  # Spiramycin
            "ncit:C839",
            "chemidplus:8025-81-8",
        },
        "chemidplus:8025-81-8": {  # Spiramycin
            "ncit:C839",
            "chemidplus:8025-81-8",
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        },
        "drugsatfda:ANDA074656": {  # Cisplatin
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        },
        "drugsatfda:ANDA074735": {  # Cisplatin
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        },
        "drugsatfda:ANDA206774": {  # Cisplatin
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        },
        "drugsatfda:ANDA207323": {  # Cisplatin
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        },
        "drugsatfda:ANDA075036": {  # Cisplatin
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
        },
        "drugsatfda:NDA018057": {  # Cisplatin
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "drugbank:DB00515",
            "drugbank:DB12117",
            "hemonc:105",
            "chembl:CHEMBL11359",
            "drugsatfda:ANDA074656",
            "drugsatfda:ANDA074735",
            "drugsatfda:ANDA206774",
            "drugsatfda:ANDA207323",
            "drugsatfda:ANDA075036",
            "drugsatfda:NDA018057"
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
        "drugsatfda:NDA210595": {
            "drugsatfda:NDA210595"
        }
    }


def test_create_record_id_set(merge_handler, record_id_groups):
    """Test creation of record ID sets. Queries DB and matches against
    record_id_groups fixture.
    """
    # try a few different orders
    keys = list(record_id_groups.keys())
    key_len = len(keys)
    order0 = list(range(key_len))
    random.seed(42)
    orders = [random.sample(order0, key_len) for _ in range(5)]
    for order in [order0] + orders:
        ordered_keys = [keys[i] for i in order]
        merge_handler.merge._groups = {}

        for record_id in ordered_keys:
            new_group = merge_handler.create_record_id_set(record_id)
            if new_group:
                for concept_id in new_group:
                    merge_handler.merge._groups[concept_id] = new_group
        groups = merge_handler.merge._groups

        # perform checks
        for concept_id in groups.keys():
            assert groups[concept_id] == record_id_groups[concept_id], f"{concept_id}"
        assert len(groups) == len(record_id_groups)  # check if any are missing

    # test dead reference
    has_dead_ref = "ncit:C107245"
    dead_group = merge_handler.create_record_id_set(has_dead_ref)
    assert dead_group == {has_dead_ref}


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
    assert "drugsatfda:NDA210595" not in updates
