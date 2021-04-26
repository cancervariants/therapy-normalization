"""Test merged record generation."""
import pytest
from therapy.etl.merge import Merge
from therapy.schemas import ApprovalStatus
from typing import Dict


@pytest.fixture(scope='module')
def merge_handler(mock_database):
    """Provide Merge instance to test cases."""
    class MergeHandler:
        def __init__(self):
            self.merge = Merge(mock_database())

        def get_merge(self):
            return self.merge

        def create_merged_concepts(self, record_ids):
            return self.merge.create_merged_concepts(record_ids)

        def get_added_records(self):
            return self.merge._database.added_records

        def get_updates(self):
            return self.merge._database.updates

        def create_record_id_set(self, record_id):
            return self.merge._create_record_id_set(record_id)

        def generate_merged_record(self, record_id_set):
            return self.merge._generate_merged_record(record_id_set)

    return MergeHandler()


def compare_merged_records(actual: Dict, fixture: Dict):
    """Check that records are identical."""
    assert actual['concept_id'] == fixture['concept_id']
    assert actual['label_and_type'] == fixture['label_and_type']
    assert ('label' in actual) == ('label' in fixture)
    if 'label' in actual or 'label' in fixture:
        assert actual['label'] == fixture['label']
    assert ('trade_names' in actual) == ('trade_names' in fixture)
    if 'trade_names' in actual or 'trade_names' in fixture:
        assert set(actual['trade_names']) == set(fixture['trade_names'])
    assert ('aliases' in actual) == ('aliases' in fixture)
    if 'aliases' in actual or 'aliases' in fixture:
        assert set(actual['aliases']) == set(fixture['aliases'])
    assert ('xrefs' in actual) == ('xrefs' in fixture)
    if 'xrefs' in actual or 'xrefs' in fixture:
        assert set(actual['xrefs']) == set(fixture['xrefs'])
    assert ('approval_status' in actual) == ('approval_status' in fixture)
    if 'approval_status' in actual or 'approval_status' in fixture:
        assert set(actual['approval_status']) == \
            set(fixture['approval_status'])
    assert ('approval_year' in actual) == ('approval_year' in fixture)
    if 'approval_year' in actual or 'approval_year' in fixture:
        assert set(actual['approval_year']) == set(fixture['approval_year'])
    assert ('fda_indication' in actual) == ('fda_indication' in fixture)
    if 'fda_indication' in actual or 'fda_indication' in fixture:
        actual_inds = actual['fda_indication'].copy()
        fixture_inds = fixture['fda_indication'].copy()
        assert len(actual_inds) == len(fixture_inds)
        actual_inds.sort(key=lambda x: x[0])
        fixture_inds.sort(key=lambda x: x[0])
        for i in range(len(actual_inds)):
            assert actual_inds[i] == fixture_inds[i]


@pytest.fixture(scope='module')
def phenobarbital_merged():
    """Create phenobarbital fixture."""
    return {
        "label_and_type": "rxcui:8134##merger",
        "concept_id": "rxcui:8134",
        "other_ids": [
            "ncit:C739",
            "drugbank:DB01174",
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
            'APRD00184',
            'Fenobarbital',
            'Luminal®',
            'PHENO',
            'Phenemal',
            'PHENOBARBITAL',
            'PHENobarbital',
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
            'Phenyläthylbarbitursäure',
            'Phenylethylbarbitursäure',
            'PHENYLETHYLMALONYLUREA',
            'Phenylethylmalonylurea',
            'Product containing phenobarbital (medicinal product)',
            'fenobarbital',
            'phenobarbital',
            'phenobarbital sodium',
            'phenylethylbarbiturate'
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
        "label": "Phenobarbital",
    }


@pytest.fixture(scope='module')
def cisplatin_merged():
    """Create cisplatin fixture."""
    return {
        "label_and_type": "rxcui:2555##merger",
        "concept_id": "rxcui:2555",
        "other_ids": [
            "ncit:C376",
            "drugbank:DB00515",
            "hemonc:105",
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
            'APRD00359',
            'CDDP',
            'CISplatin',
            'Cisplatin',
            'Cis-DDP',
            'CIS-DDP',
            'DACP',
            'DDP',
            'Diamminodichloride, Platinum',
            'Dichlorodiammineplatinum',
            'Platinum Diamminodichloride',
            'cis Diamminedichloroplatinum',
            'cis Platinum',
            'cis-Diaminedichloroplatinum',
            'cis-Diamminedichloroplatinum',
            'cis-diamminedichloroplatinum(II)',
            'cis-Diamminedichloroplatinum(II)',
            'cis-Dichlorodiammineplatinum(II)',
            'cisplatinum',
            'cis-Platinum',
            'cis-platinum',
            'cisplatino',
            'cis-diamminedichloroplatinum(II)',
            'cis-diamminedichloroplatinum III',
            'NSC 119875',
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
            "pubchem.compound:5702198",
            "unii:Q20Q21Q62J",
            "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L"
        ],
        "approval_status": ApprovalStatus.APPROVED,
        "approval_year": ["1978"],
        "fda_indication": [
            ["hemonc:671", "Testicular cancer", "ncit:C7251"],
            ["hemonc:645", "Ovarian cancer", "ncit:C7431"],
            ["hemonc:569", "Bladder cancer", "ncit:C9334"]
        ],
    }


@pytest.fixture(scope='module')
def spiramycin_merged():
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return {
        "label_and_type": "ncit:c839##merger",
        "concept_id": "ncit:C839",
        "other_ids": [
            'chemidplus:8025-81-8',
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
        "xrefs": [
            "umls:C0037962",
            "fda:71ODY0V87H",
        ],
    }


@pytest.fixture(scope='module')
def bendamustine_merged():
    """Create fixture for bendamustine. Should only contain a single HemOnc
    record.
    """
    return {
        "label_and_type": "hemonc:65##merger",
        "concept_id": "hemonc:65",
        "label": "Bendamustine",
        "aliases": [
            "CEP-18083",
            "cytostasan hydrochloride",
            "SyB L-0501",
            "SDX-105",
            "bendamustine hydrochloride",
            "bendamustin hydrochloride"
        ],
        "other_identifiers": ["rxcui:134547"],
        "trade_names": [
            "Bendamax",
            "Bendawel",
            "Bendeka",
            "Bendit",
            "Innomustine",
            "Leuben",
            "Levact",
            "Maxtorin",
            "MyMust",
            "Purplz",
            "Ribomustin",
            "Treakisym",
            "Treanda",
            "Xyotin"
        ],
        "approval_status": ApprovalStatus.APPROVED,
        "approval_year": ["2008", "2015"],
        "fda_indication": [
            ["hemonc:581", "Chronic lymphocytic leukemia", "ncit:C3163"],
            ["hemonc:46094", "Indolent lymphoma", "ncit:C8504"]
        ]
    }


@pytest.fixture(scope='module')
def record_id_groups():
    """Create fixture for concept group sets."""
    return {
        "drugbank:DB01174": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174"
        },
        "rxcui:8134": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174"
        },
        "chemidplus:50-06-6": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174"
        },
        "ncit:C739": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174"
        },
        "wikidata:Q407241": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
            "drugbank:DB01174"
        },
        "chemidplus:8025-81-8": {
            "ncit:C839",
            "chemidplus:8025-81-8",
        },
        "ncit:C839": {
            "ncit:C839",
            "chemidplus:8025-81-8",
        },
        "chemidplus:15663-27-1": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "rxcui:2555": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "ncit:C376": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "wikidata:Q412415": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "wikidata:Q47522001": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "drugbank:DB00515": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "hemonc:105": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415",
            "wikidata:Q47522001",
            "drugbank:DB00515",
            "hemonc:105"
        },
        "rxcui:4126": {
            "rxcui:4126",
            "wikidata:Q47521576",
            "drugbank:DB01143"
        },
        "wikidata:Q47521576": {
            "rxcui:4126",
            "wikidata:Q47521576",
            "drugbank:DB01143"
        },
        "drugbank:DB01143": {
            "rxcui:4126",
            "wikidata:Q47521576",
            "drugbank:DB01143"
        },
        "hemonc:65": {
            "hemonc:65"
        }
    }


def test_create_record_id_set(merge_handler, record_id_groups):
    """Test creation of record ID sets. Queries DB and matches against
    record_id_groups fixture.
    """
    for record_id in record_id_groups.keys():
        new_group = merge_handler.create_record_id_set(record_id)
        for concept_id in new_group:
            merge_handler.merge._groups[concept_id] = new_group
    groups = merge_handler.merge._groups

    for concept_id in groups.keys():
        assert groups[concept_id] == record_id_groups[concept_id]
    assert len(groups) == len(record_id_groups)  # check if any are missing

    # test dead reference
    has_dead_ref = 'ncit:C107245'
    dead_group = merge_handler.create_record_id_set(has_dead_ref)
    assert dead_group == {has_dead_ref}


def test_generate_merged_record(merge_handler, record_id_groups,
                                phenobarbital_merged, cisplatin_merged,
                                spiramycin_merged, bendamustine_merged):
    """Test generation of merged record method."""
    phenobarbital_ids = record_id_groups['rxcui:8134']
    merge_response = merge_handler.generate_merged_record(phenobarbital_ids)
    compare_merged_records(merge_response, phenobarbital_merged)

    cisplatin_ids = record_id_groups['rxcui:2555']
    merge_response = merge_handler.generate_merged_record(cisplatin_ids)
    compare_merged_records(merge_response, cisplatin_merged)

    spiramycin_ids = record_id_groups['ncit:C839']
    merge_response = merge_handler.generate_merged_record(spiramycin_ids)
    compare_merged_records(merge_response, spiramycin_merged)

    bendamustin_ids = record_id_groups['hemonc:65']
    merge_response = merge_handler.generate_merged_record(bendamustin_ids)
    compare_merged_records(merge_response, bendamustine_merged)


def test_create_merged_concepts(merge_handler, record_id_groups,
                                phenobarbital_merged, cisplatin_merged,
                                spiramycin_merged, bendamustine_merged):
    """Test end-to-end creation and upload of merged concepts."""
    record_ids = record_id_groups.keys()
    merge_handler.create_merged_concepts(record_ids)

    # check merged record generation and storage
    added_records = merge_handler.get_added_records()
    assert len(added_records) == 5

    phenobarb_merged_id = phenobarbital_merged['concept_id']
    assert phenobarb_merged_id in added_records.keys()
    compare_merged_records(added_records[phenobarb_merged_id],
                           phenobarbital_merged)

    cispl_merged_id = cisplatin_merged['concept_id']
    assert cispl_merged_id in added_records.keys()
    compare_merged_records(added_records[cispl_merged_id], cisplatin_merged)

    spira_merged_id = spiramycin_merged['concept_id']
    assert spira_merged_id in added_records.keys()
    compare_merged_records(added_records[spira_merged_id],
                           spiramycin_merged)

    benda_merged_id = bendamustine_merged['concept_id']
    assert benda_merged_id in added_records.keys()
    compare_merged_records(added_records[benda_merged_id],
                           bendamustine_merged)

    # check merged record reference updating
    updates = merge_handler.get_updates()
    assert len(updates) == len(record_id_groups)
    for concept_id in record_id_groups['rxcui:8134']:
        assert updates[concept_id] == {
            'merge_ref': phenobarbital_merged['concept_id'].lower()
        }
    for concept_id in record_id_groups['rxcui:2555']:
        assert updates[concept_id] == {
            'merge_ref': cisplatin_merged['concept_id'].lower()
        }
    for concept_id in record_id_groups['ncit:C839']:
        assert updates[concept_id] == {
            'merge_ref': spiramycin_merged['concept_id'].lower()
        }
    for concept_id in record_id_groups['hemonc:65']:
        assert updates[concept_id] == {
            'merge_ref': bendamustine_merged['concept_id'].lower()
        }
