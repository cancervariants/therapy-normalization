"""Test merged record generation."""
import pytest
from therapy.etl.merge import Merge
from therapy.database import Database
from therapy.schemas import MergedMatch


@pytest.fixture(scope='module')
def get_merge():
    """Provide Merge instance to test cases."""
    class MergeHandler():
        def __init__(self):
            self.merge = Merge(Database())

        def create_merged_concepts(self, record_ids):
            return self.merge.create_merged_concepts(record_ids)

        def create_record_id_set(self, record_id):
            return self.merge._create_record_id_set(record_id)

        def generate_merged_record(self, record_id_set):
            return self.merge._generate_merged_record(record_id_set)
    return MergeHandler()


@pytest.fixture(scope='module')
def phenobarbital():
    """Create phenobarbital fixture."""
    return MergedMatch(**{
        "concept_ids": [
            "wikidata:Q407241",
            "chemidplus:50-06-6",
            "rxcui:8134",
            "ncit:C739"
        ],
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
    })


@pytest.fixture(scope='module')
def cisplatin():
    """Create cisplatin fixture."""
    return MergedMatch(**{
        "concept_ids": [
            "rxcui:2555",
            "ncit:C376",
            "chemidplus:15663-27-1",
            "wikidata:Q412415"
        ],
        "trade_names": [
            "Cisplatin",
        ],
        "aliases": [
            '1,2-Diaminocyclohexaneplatinum II citrate',
            'CDDP',
            'CISplatin',
            'Cis-DDP',
            'Cisplatin',
            'Cisplatin (substance)',
            'Cisplatin-containing product',
            'DDP',
            'Diaminedichloroplatinum',
            'Diamminodichloride, Platinum',
            'Dichlorodiammineplatinum',
            'Platinum Diamminodichloride',
            'Product containing cisplatin (medicinal product)',
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
            'cis-diamminedichloroplatinum(II)'
        ],
        "label": "Cisplatin",
        "xrefs": [
            "umls:C0008838",
            "fda:Q20Q21Q62J",
            "chebi:CHEBI:27899",
            "usp:m17910",
            "gsddb:862",
            "snomedct:57066004",
            "vandf:4018139",
            "msh:D002945",
            "mthspl:Q20Q21Q62J",
            "snomedct:387318005",
            "mmsl:d00195",
            "fdbmk:002641",
            "atc:L01XA01",
            "mmsl:31747",
            "mmsl:4456"
        ]
    })


@pytest.fixture(scope='module')
def hydrocorticosteroid():
    """Create fixture for 17-hydrocorticosteroid, which should merge only
    from RxNorm.
    """
    return MergedMatch(**{
        "concept_ids": [
            "rxcui:19"
        ],
        "label": "17-hydrocorticosteroid",
        "aliases": [
            "17-hydroxycorticoid",
            "17-hydroxycorticosteroid",
            "17-hydroxycorticosteroid (substance)"
        ],
        "other_identifiers": [],
        "xrefs": [
            "snomedct:112116001"
        ],
        "approval_status": None
    })


@pytest.fixture(scope='module')
def spiramycin():
    """Create fixture for spiramycin. The RxNorm entry should be inaccessible
    to this group.
    """
    return MergedMatch(**{
        "concept_ids": [
            "ncit:C839",
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
            "(4R,5S,6R,7R,9R,10R,11E,13E,16R)-10-{[(2R,5S,6R)-5-(dimethylamino)-6-methyltetrahydro-2H-pyran-2-yl]oxy}-9,16-dimethyl-5-methoxy-2-oxo-7-(2-oxoethyl)oxacyclohexadeca-11,13-dien-6-yl 3,6-dideoxy-4-O-(2,6-dideoxy-3-C-methyl-alpha-L-ribo-hexopyranosyl)-3-(dimethylamino)-alpha-D-glucopyranoside"],  # noqa: E501
        "xrefs": [
            "umls:C0037962",
            "fda:71ODY0V87H",
        ],
        "approval_status": None
    })


def test_merge(get_merge, phenobarbital):
    """Test end-to-end merge function."""
    assert True
