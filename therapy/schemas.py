"""This module contains data models for representing VICC normalized
therapy records.
"""
from typing import List, Optional, Dict, Union, Any, Type
from pydantic import BaseModel, StrictBool
from enum import Enum, IntEnum


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
    """Define string constraints for use in Match Type attributes."""

    CONCEPT_ID = 100
    LABEL = 80
    TRADE_NAME = 80
    ALIAS = 60
    FUZZY_MATCH = 20
    NO_MATCH = 0


class SourceName(Enum):
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


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs."""

    CASREGISTRY = "chemidplus"
    CHEMIDPLUS = "chemidplus"
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
    ATC = "atc"
    CVX = "cvx"
    GS = "gsddb"  # Gold Standard Drug Database
    MDDB = "mmddb"  # Medi-Span Master Drug Database
    MMSL = "mmsl"  # Multum MediSource Lexicon
    MMX = "mmx"  # Micromedex RED BOOK
    MSH = "msh"  # Medical Subject Headings
    MTHCMSFRF = "mthcmsfrf"  # CMS Formulary Reference File
    MTHSPL = "mthspl"  # FDA Structured Product Labels
    NDDF = "fdbmk"  # FDB MedKnowledge (Formerly NDDF Plus)
    SNOMEDCT_US = "snomedct"  # US Edition of SNOMED CT
    USP = "usp"  # USP Compendial Nomenclature
    VANDF = "vandf"  # Veterans Health Administration National Drug File


class DataLicenseAttributes(BaseModel):
    """Define constraints for data license attributes."""

    non_commercial: StrictBool
    share_alike: StrictBool
    attribution: StrictBool


class Meta(BaseModel):
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
                         model: Type['Meta']) -> None:
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
    meta_: Meta

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
                'meta_': {
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
    meta_: Meta

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
                'meta_': {
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


class Service(BaseModel):
    """Core response schema containing matches for each source"""

    query: str
    warnings: Optional[Dict]
    source_matches: Union[Dict[SourceName, MatchesKeyed], List[MatchesListed]]

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Configure OpenAPI schema"""
            if 'title' in schema.keys():
                schema.pop('title', None)
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
            schema['example'] = {
                'query': 'CISPLATIN',
                'warnings': None,
                'meta_': {
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
            }


class MergedDrug(BaseModel):
    """Structure of merged drug object as provided to end user."""

    concept_id_group: List[str]
    label: Optional[str]
    approval_status: Optional[ApprovalStatus]
    trade_names: Optional[List[str]]
    aliases: Optional[List[str]]
    xrefs: Optional[List[str]]


class MergedService(BaseModel):
    """Response schema for grouped matching endpoint."""

    query: str
    warnings: Optional[Dict]
    match_type: MatchType
    match: Optional[MergedDrug]
