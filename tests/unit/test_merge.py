"""Test merged record generation."""
import pytest
from therapy.etl.merge import Merge
from typing import Dict


@pytest.fixture(scope='module')
def merge_handler(mock_database):
    """Provide Merge instance to test cases."""
    class MergeHandler():
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


def compare_merged_records(actual_record: Dict, fixture_record: Dict):
    """Check that records are identical."""
    assert actual_record['concept_id'] == fixture_record['concept_id']
    assert actual_record['label_and_type'] == fixture_record['label_and_type']
    assert ('label' in actual_record) == ('label' in fixture_record)
    if 'label' in actual_record or 'label' in fixture_record:
        assert actual_record['label'] == fixture_record['label']
    assert ('trade_names' in actual_record) == ('trade_names' in fixture_record)  # noqa: E501
    if 'trade_names' in actual_record or 'trade_names' in fixture_record:
        assert set(actual_record['trade_names']) == set(fixture_record['trade_names'])  # noqa: E501
    assert ('aliases' in actual_record) == ('aliases' in fixture_record)
    if 'aliases' in actual_record or 'aliases' in fixture_record:
        assert set(actual_record['aliases']) == set(fixture_record['aliases'])
    assert ('xrefs' in actual_record) == ('xrefs' in fixture_record)
    if 'xrefs' in actual_record or 'xrefs' in fixture_record:
        assert set(actual_record['xrefs']) == set(fixture_record['xrefs'])


@pytest.fixture(scope='module')
def phenobarbital_merged():
    """Create phenobarbital fixture."""
    return {
        "label_and_type": "rxcui:8134|ncit:c739|chemidplus:50-06-6|wikidata:q407241##merger",  # noqa: E501
        "concept_id": "rxcui:8134|ncit:C739|chemidplus:50-06-6|wikidata:Q407241",  # noqa: E501
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
def cisplatin_merged():
    """Create cisplatin fixture."""
    return {
        "label_and_type": "rxcui:2555|ncit:c376|chemidplus:15663-27-1|wikidata:q412415##merger",  # noqa: E501
        "concept_id": "rxcui:2555|ncit:C376|chemidplus:15663-27-1|wikidata:Q412415",  # noqa: E501
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
            'Diaminedichloroplatinum',
            'Diamminodichloride, Platinum',
            'Dichlorodiammineplatinum',
            'Platinum Diamminodichloride',
            'cis Diamminedichloroplatinum',
            'cis Platinum',
            'cis-DDP',
            'cis-Diaminedichloroplatinum',
            'cis-Diamminedichloroplatinum',
            'cis-Diamminedichloroplatinum(II)',
            'cis-Dichlorodiammineplatinum(II)',
            'cis-Platinum',
            'cis-Platinum II',
            'cis-Platinum compound',
            'cis-diamminedichloroplatinum(II)',
            'Platinol-AQ',
            'Platinol'
        ],
        "label": "cisplatin",
        "xrefs": [
            "umls:C0008838",
            "fda:Q20Q21Q62J",
            "usp:m17910",
            "snomedct:57066004",
            "vandf:4018139",
            "msh:D002945",
            "mthspl:Q20Q21Q62J",
            "snomedct:387318005",
            "mmsl:d00195",
            "atc:L01XA01",
            "mmsl:31747",
            "mmsl:4456",
            "pubchem.compound:5702198"
        ]
    }


@pytest.fixture(scope='module')
def spiramycin_merged():
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return {
        "label_and_type": "ncit:c839|chemidplus:8025-81-8##merger",
        "concept_id": "ncit:C839|chemidplus:8025-81-8",
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
def record_id_groups():
    """Create fixture for concept group sets."""
    return {
        "chemidplus:50-06-6": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
        },
        "ncit:C739": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
        },
        "ncit:C839": {
            "ncit:C839",
            "chemidplus:8025-81-8",
        },
        "rxcui:8134": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
        },
        "wikidata:Q407241": {
            "rxcui:8134",
            "ncit:C739",
            "chemidplus:50-06-6",
            "wikidata:Q407241",
        },
        "chemidplus:15663-27-1": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415"
        },
        "chemidplus:8025-81-8": {
            "ncit:C839",
            "chemidplus:8025-81-8",
        },
        "rxcui:2555": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415"
        },
        "ncit:C376": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415"
        },
        "wikidata:Q412415": {
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415"
        },
    }


def test_create_record_id_set(merge_handler, record_id_groups):
    """Test creation of record ID sets."""
    for record_id in record_id_groups.keys():
        new_group = merge_handler.create_record_id_set(record_id)
        for concept_id in new_group:
            merge_handler.merge._groups[concept_id] = new_group
    groups = merge_handler.merge._groups
    assert len(groups) == len(record_id_groups)
    for concept_id in groups.keys():
        assert groups[concept_id] == record_id_groups[concept_id]

    # test dead reference
    dead_group = merge_handler.create_record_id_set('ncit:c000000')
    assert {'ncit:c000000'} == dead_group


def test_generate_merged_record(merge_handler, record_id_groups,
                                phenobarbital_merged, cisplatin_merged,
                                spiramycin_merged):
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

    # test correct handling of dead references
    spiramycin_plus_fake = spiramycin_ids.copy()
    spiramycin_plus_fake.add('ncit:zzzzzzz')


def test_create_merged_concepts(merge_handler, record_id_groups,
                                phenobarbital_merged, cisplatin_merged,
                                spiramycin_merged):
    """Test end-to-end creation and upload of merged concepts."""
    record_ids = record_id_groups.keys()
    merge_handler.create_merged_concepts(record_ids)

    # check merged record generation and storage
    added_records = merge_handler.get_added_records()
    assert len(added_records) == 3
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

    # check merged record reference updating
    updates = merge_handler.get_updates()
    assert len(updates) == len(record_id_groups)
    for concept_id in record_id_groups['rxcui:8134']:
        assert updates[concept_id] == {
            'merge_ref': phenobarbital_merged['label_and_type']
        }
    for concept_id in record_id_groups['rxcui:2555']:
        assert updates[concept_id] == {
            'merge_ref': cisplatin_merged['label_and_type']
        }
    for concept_id in record_id_groups['ncit:C839']:
        assert updates[concept_id] == {
            'merge_ref': spiramycin_merged['label_and_type']
        }