"""This module contains data models for representing VICC normalized
therapy records.
"""
from typing import List, Optional, Dict, Union, Any, Type
from pydantic import BaseModel, StrictBool
from enum import Enum, IntEnum
from datetime import datetime


class Therapy(BaseModel):
    """A procedure or substance used in the treatment of a disease."""

    label: str
    concept_id: str
    aliases: Optional[List[str]]
    other_identifiers: Optional[List[str]]
    xrefs: Optional[List[str]]

    class Config:
        """Configure model"""

        orm_mode = True

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Therapy']) -> None:
            """Configure OpenAPI schema"""
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)


class ApprovalStatus(str, Enum):
    """Define string constraints for approval status attribute."""

    WITHDRAWN = "withdrawn"
    APPROVED = "approved"
    INVESTIGATIONAL = "investigational"


class PhaseEnum(IntEnum):
    """An enumerated drug development phase type."""

    preclinical = 0
    phase_i_trials = 1
    phase_ii_trials = 2
    phase_iii_trials = 3
    approved = 4


class Drug(Therapy):
    """A pharmacologic substance used to treat a medical condition."""

    approval_status: Optional[ApprovalStatus]
    trade_names: Optional[List[str]]
    label: Optional[str]

    class Config:
        """Enables orm_mode"""

        orm_mode = True

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Drug']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                'label': 'CISPLATIN',
                'concept_identifier': 'chembl:CHEMBL11359',
                'aliases': [
                    'Cisplatin',
                    'Cis-Platinum II',
                    'Cisplatinum',
                    'cis-diamminedichloroplatinum(II)',
                    'CIS-DDP',
                    'INT-230-6 COMPONENT CISPLATIN',
                    'INT230-6 COMPONENT CISPLATIN',
                    'NSC-119875',
                    'Platinol',
                    'Platinol-Aq'
                ],
                'other_identifiers': [],
                'trade_name': [
                    'PLATINOL',
                    'PLATINOL-AQ',
                    'CISPLATIN'
                ]
            }


class DrugGroup(Therapy):
    """A grouping of drugs based on common pharmacological attributes."""

    description: str
    type_identifier: str
    drugs: List[Drug]


class MatchType(IntEnum):
    """Define string constraints for use in Match Type attributes.

    Concept_ID=100; Label=80; Trade Name=80; Alias=60; Fuzzy=20; No Match=0
    """

    CONCEPT_ID = 100
    LABEL = 80
    TRADE_NAME = 80
    ALIAS = 60
    OTHER_ID = 60
    XREF = 60
    FUZZY_MATCH = 20
    NO_MATCH = 0


class SourcePriority(IntEnum):
    """Define constraints for Source Priority Rankings."""

    RXNORM = 1
    NCIT = 2
    CHEMIDPLUS = 5
    WIKIDATA = 6


class SourceName(str, Enum):
    """Define string constraints to ensure consistent capitalization."""

    WIKIDATA = "Wikidata"
    CHEMBL = "ChEMBL"
    NCIT = "NCIt"
    DRUGBANK = "DrugBank"
    CHEMIDPLUS = "ChemIDplus"
    RXNORM = "RxNorm"


class SourceIDAfterNamespace(Enum):
    """Define string constraints after namespace."""

    WIKIDATA = "Q"
    CHEMBL = "CHEMBL"
    DRUGBANK = "DB"
    NCIT = "C"
    CHEMIDPLUS = ""
    RXNORM = ""


class ProhibitedSources(Enum):
    """Define constraints for sources that are prohibited in normalize
    endpoint.
    """

    CHEMBL = SourceName.CHEMBL.value
    DRUGBANK = SourceName.DRUGBANK.value


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs."""

    CHEMIDPLUS = "chemidplus"
    CASREGISTRY = CHEMIDPLUS
    PUBCHEMCOMPOUND = "pubchem.compound"
    PUBCHEMSUBSTANCE = "pubchem.substance"
    CHEMBL = "chembl"
    RXNORM = "rxcui"
    DRUGBANK = "drugbank"
    WIKIDATA = "wikidata"
    NCIT = "ncit"
    FDA = "fda"
    ISO = "iso"
    UMLS = "umls"
    CHEBI = "chebi"
    KEGGCOMPOUND = "kegg.compound"
    KEGGDRUG = "kegg.drug"
    BINDINGDB = "bindingdb"
    PHARMGKB = "pharmgkb.drug"
    CHEMSPIDER = "chemspider"
    ZINC = "zinc"
    PDB = "pdb"
    THERAPEUTICTARGETSDB = "ttd"
    IUPHAR = "iuphar"
    GUIDETOPHARMACOLOGY = "gtopdb"
    ATC = "atc"  # Anatomical Therapeutic Chemical Classification System
    CVX = "cvx"  # Vaccines Administered
    MMSL = "mmsl"  # Multum MediSource Lexicon
    MSH = "mesh"  # Medical Subject Headings
    MTHCMSFRF = "mthcmsfrf"  # CMS Formulary Reference File
    MTHSPL = "mthspl"  # FDA Structured Product Labels
    USP = "usp"  # USP Compendial Nomenclature
    VANDF = "vandf"  # Veterans Health Administration National Drug File


class DataLicenseAttributes(BaseModel):
    """Define constraints for data license attributes."""

    non_commercial: StrictBool
    share_alike: StrictBool
    attribution: StrictBool


class SourceMeta(BaseModel):
    """Metadata for a given source to return in response object."""

    data_license: str
    data_license_url: str
    version: str
    data_url: Optional[str]
    rdp_url: Optional[str]
    data_license_attributes: Dict[str, StrictBool]

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['SourceMeta']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                'data_license': 'CC BY-SA 3.0',
                'data_license_url':
                    'https://creativecommons.org/licenses/by-sa/3.0/',
                'version': '27',
                'data_url':
                    'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/',  # noqa: E501
                'rdp_url': 'http://reusabledata.org/chembl.html',
                'data_license_attributes': {
                    'non_commercial': False,
                    'share_alike': True,
                    'attribution': True
                }
            }


class MatchesKeyed(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an object, not an array.
    """

    match_type: MatchType
    records: List[Drug]
    source_meta_: SourceMeta

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['MatchesKeyed']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                'match_type': 0,
                'records': [],
                'source_meta_': {
                    'data_license': 'CC BY-SA 3.0',
                    'data_license_url':
                        'https://creativecommons.org/licenses/by-sa/3.0/',
                    'version': '27',
                    'data_url':
                        'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/',  # noqa: E501
                    'rdp_url': 'http://reusabledata.org/chembl.html',
                    'data_license_attributes': {
                        'non_commercial': False,
                        'share_alike': True,
                        'attribution': True
                    }
                },
            }


class MatchesListed(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an array, not an object.
    """

    source: SourceName
    match_type: MatchType
    records: List[Drug]
    source_meta_: SourceMeta

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['MatchesListed']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                'normalizer': 'ChEMBL',
                'match_type': 0,
                'records': [],
                'source_meta_': {
                    'data_license': 'CC BY-SA 3.0',
                    'data_license_url':
                        'https://creativecommons.org/licenses/by-sa/3.0/',
                    'version': '27',
                    'data_url':
                        'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/',  # noqa: E501
                    'rdp_url': 'http://reusabledata.org/chembl.html',
                    'data_license_attributes': {
                        'non_commercial': False,
                        'share_alike': True,
                        'attribution': True
                    }
                },
            }


class Extension(BaseModel):
    """Value Object Descriptor Extension class."""

    type: str
    name: str
    value: Union[bool, List[str]]


class ValueObjectDescriptor(BaseModel):
    """VOD class. Provide additional Extension classes for trade_names and
    references to non-normalized sources.
    """

    id: str
    type: str
    value: Any
    label: str
    xrefs: Optional[List[str]]
    alternate_labels: Optional[List[str]]
    extensions: Optional[List[Extension]]


class ServiceMeta(BaseModel):
    """Metadata regarding the therapy-normalization service."""

    name = "thera-py"
    version: str
    response_datetime: datetime
    url: str

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['SourceMeta']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                'name': 'thera-py',
                'version': '0.1.0',
                'response_datetime': '2021-04-05T16:44:15.367831',
                'url': 'https://github.com/cancervariants/therapy-normalization'  # noqa: E501
            }


class NormalizationService(BaseModel):
    """Response containing one or more merged records and source data."""

    query: str
    warnings: Optional[Dict]
    match_type: MatchType
    value_object_descriptor: Optional[ValueObjectDescriptor]
    source_meta_: Optional[Dict[SourceName, SourceMeta]]
    service_meta_: ServiceMeta

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['NormalizationService']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                "query": "cisplatin",
                "warnings": None,
                "match_type": 80,
                "value_object_descriptor": {
                    "id": "normalize.therapy:cisplatin",
                    "type": "TherapyDescriptor",
                    "value": {
                        "type": "Therapy",
                        "id": "rxcui:2555"
                    },
                    "label": "cisplatin",
                    "xrefs": [
                        "ncit:C376", "chemidplus:15663-27-1",
                        "wikidata:Q412415"
                    ],
                    "alternate_labels": [
                        "CIS-DDP", "cis Platinum", "DDP",
                        "Dichlorodiammineplatinum",
                        "1,2-Diaminocyclohexaneplatinum II citrate",
                        "CISplatin",
                        "cis Diamminedichloroplatinum",
                        "CDDP",
                        "Diamminodichloride, Platinum",
                        "cis-Dichlorodiammineplatinum(II)",
                        "cis-Platinum",
                        "cis-diamminedichloroplatinum(II)",
                        "cis-Diamminedichloroplatinum(II)",
                        "Cis-DDP",
                        "cis-Diamminedichloroplatinum",
                        "cis-Diaminedichloroplatinum",
                        "Platinol-AQ", "Platinol",
                        "Platinum Diamminodichloride"
                    ],
                    "extensions": [
                        {
                            "type": "Extension",
                            "name": "trade_names",
                            "value": [
                                "Platinol", "Cisplatin"
                            ]
                        },
                        {
                            "type": "Extension",
                            "name": "associated_with",
                            "value": [
                                "atc:L01XA01",
                                "mmsl:4456",
                                "chebi:CHEBI:27899",
                                "pubchem.compound:5702198",
                                "umls:C0008838",
                                "usp:m17910",
                                "fda:Q20Q21Q62J",
                                "mmsl:d00195",
                                "mthspl:Q20Q21Q62J",
                                "mmsl:31747",
                                "mesh:D002945",
                                "vandf:4018139"
                            ]
                        }
                    ]
                },
                "source_meta_": {
                    "RxNorm": {
                        "data_license": "UMLS Metathesaurus",
                        "data_license_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",  # noqa: E501
                        "version": "20210104",
                        "data_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html",  # noqa: E501
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False
                        }
                    },
                    "NCIt": {
                        "data_license": "CC BY 4.0",
                        "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa: E501
                        "version": "20.09d",
                        "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",  # noqa: E501
                        "rdp_url": "http://reusabledata.org/ncit.html",
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False
                        }
                    },
                    "ChemIDplus": {
                        "data_license": "custom",
                        "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                        "version": "20200327", "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",  # noqa: E501
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False
                        }
                    },
                    "Wikidata": {
                        "data_license": "CC0 1.0",
                        "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",  # noqa: E501
                        "version": "20200812",
                        "data_url": None,
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": False,
                            "share_alike": False
                        }
                    }
                },
                "service_meta_": {
                    'name': 'thera-py',
                    'version': '0.1.0',
                    'response_datetime': '2021-04-05T16:44:15.367831',
                    'url': 'https://github.com/cancervariants/therapy-normalization'  # noqa: E501
                }
            }


class SearchService(BaseModel):
    """Core response schema containing matches for each source"""

    query: str
    warnings: Optional[Dict]
    source_matches: Union[Dict[SourceName, MatchesKeyed], List[MatchesListed]]
    service_meta_: ServiceMeta

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['SearchService']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                "query": "cisplatin",
                "warnings": None,
                "source_matches": [
                    {
                        "source": "ChemIDplus",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "Cisplatin",
                                "concept_id": "chemidplus:15663-27-1",
                                "aliases": [
                                    "cis-Diaminedichloroplatinum",
                                    "1,2-Diaminocyclohexaneplatinum II citrate"
                                ],
                                "other_identifiers": ["drugbank:DB00515"],
                                "xrefs": ["fda:Q20Q21Q62J"],
                                "approval_status": None,
                                "trade_names": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "custom",
                            "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                            "version": "20210204",
                            "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",  # noqa: E501
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False
                            }
                        }
                    },
                    {
                        "source": "RxNorm",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "cisplatin",
                                "concept_id": "rxcui:2555",
                                "aliases": [
                                    "cis-Dichlorodiammineplatinum(II)",
                                    "Platinum Diamminodichloride",
                                    "cis Diamminedichloroplatinum",
                                    "cis-diamminedichloroplatinum(II)",
                                    "cis-Diamminedichloroplatinum",
                                    "cis Platinum",
                                    "CDDP",
                                    "Dichlorodiammineplatinum",
                                    "cis-Platinum",
                                    "CISplatin",
                                    "cis-Diamminedichloroplatinum(II)",
                                    "Cis-DDP",
                                    "DDP",
                                    "Diamminodichloride, Platinum"
                                ],
                                "other_identifiers": [
                                    "drugbank:DB00515",
                                    "drugbank:DB12117"
                                ],
                                "xrefs": [
                                    "usp:m17910",
                                    "vandf:4018139",
                                    "mesh:D002945",
                                    "mthspl:Q20Q21Q62J",
                                    "mmsl:d00195",
                                    "atc:L01XA01",
                                    "mmsl:31747",
                                    "mmsl:4456"
                                ],
                                "approval_status": "approved",
                                "trade_names": [
                                    "Cisplatin",
                                    "Platinol"
                                ]
                            }
                        ],
                        "source_meta_": {
                            "data_license": "UMLS Metathesaurus",
                            "data_license_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",  # noqa: E501
                            "version": "20210104",
                            "data_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html",  # noqa: E501
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False
                            }
                        }
                    },
                    {
                        "source": "NCIt",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "Cisplatin",
                                "concept_id": "ncit:C376",
                                "aliases": [],
                                "other_identifiers": ["chemidplus:15663-27-1"],
                                "xrefs": [
                                    "umls:C0008838",
                                    "fda:Q20Q21Q62J",
                                    "chebi:CHEBI:27899"
                                ],
                                "approval_status": None,
                                "trade_names": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa: E501
                            "version": "20.09d",
                            "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/2020/20.09d_Release/",  # noqa: E501
                            "rdp_url": "http://reusabledata.org/ncit.html",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False
                            }
                        }
                    },
                    {
                        "source": "Wikidata",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "cisplatin",
                                "concept_id": "wikidata:Q412415",
                                "aliases": [
                                    "Platinol",
                                    "cis-diamminedichloroplatinum(II)",
                                    "CDDP",
                                    "Cis-DDP",
                                    "CIS-DDP",
                                    "Platinol-AQ"
                                ],
                                "other_identifiers": [
                                    "chemidplus:15663-27-1",
                                    "chembl:CHEMBL11359",
                                    "rxcui:2555",
                                    "drugbank:DB00515"
                                ],
                                "xrefs": ["pubchem.compound:5702198"],
                                "approval_status": None,
                                "trade_names": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC0 1.0",
                            "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",  # noqa: E501
                            "version": "20210331",
                            "data_url": None,
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": False,
                                "share_alike": False
                            }
                        }
                    }
                ],
                "service_meta_": {
                    'name': 'thera-py',
                    'version': '0.1.0',
                    'response_datetime': '2021-04-05T16:44:15.367831',
                    'url': 'https://github.com/cancervariants/therapy-normalization'  # noqa: E501
                }
            }
