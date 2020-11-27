"""This module contains data models for representing VICC normalized
therapy records.
"""
from typing import List, Optional, Dict, Union, Any, Type
from pydantic import BaseModel
from enum import Enum, IntEnum


class Therapy(BaseModel):
    """A procedure or substance used in the treatment of a disease."""

    label: str
    concept_id: str
    aliases: List[str]
    other_identifiers: List[str]

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
    PRIMARY_LABEL = 80
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


class SourceIDAfterNamespace(Enum):
    """Define string constraints after namespace."""

    WIKIDATA = "Q"
    CHEMBL = "CHEMBL"
    DRUGBANK = "DB"
    NCIT = "C"
    CASREGISTRY = ""
    PUBCHEMCOMPOUND = ""
    PUBCHEMSUBSTANCE = ""
    RXNORM = ""
    UMLS = ""
    CHEBI = ""
    KEGGCOMPOUND = ""
    KEGGDRUG = ""
    BINDINGDB = ""
    PHARMGKB = ""
    CHEMSPIDER = ""
    ZINC = ""
    PDB = ""
    THERAPEUTICTARGETSDB = ""
    IUPHAR = ""
    GUIDETOPHARMACOLOGY = ""


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs."""

    CASREGISTRY = "chemidplus"
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


class Meta(BaseModel):
    """Metadata for a given source to return in response object."""

    data_license: str
    data_license_url: str
    version: str
    data_url: Optional[str]

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
                    'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'  # noqa: E501
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
                        'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'  # noqa: E501
                }
            }
