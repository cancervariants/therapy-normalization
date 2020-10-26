"""This module contains data models for representing VICC normalized
therapy records.
"""
from typing import List, Optional, Dict, Union, Any, Type
from pydantic import BaseModel, Field
from enum import Enum, IntEnum


class Therapy(BaseModel):
    """A procedure or substance used in the treatment of a disease."""

    label: str
    concept_identifier: str
    aliases: List[str]
    other_identifiers: List[str]

    class Config:
        """Configure model"""

        orm_mode = True

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Customize fields in OpenAPI JSON output"""
            if schema.get('title', None):
                schema.pop('title')
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)


class PhaseEnum(IntEnum):
    """An enumerated drug development phase type."""

    preclinical = 0
    phase_i_trials = 1
    phase_ii_trials = 2
    phase_iii_trials = 3
    approved = 4


class Drug(Therapy):
    """A pharmacologic substance used to treat a medical condition."""

    max_phase: Optional[PhaseEnum]
    withdrawn: Optional[bool]
    trade_name: Optional[List[str]]
    label: Optional[str]

    class Config:
        """Enables orm_mode"""

        orm_mode = True

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Customize fields in OpenAPI JSON output"""
            if schema.get('title', None):
                schema.pop('title')
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)


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


class SourceIDAfterNamespace(Enum):
    """Define string constraints after namespace."""

    WIKIDATA = "Q"
    CHEMBL = "CHEMBL"


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs."""

    CASREGISTRY = "chemidplus"
    PUBCHEMCOMPOUND = "pubchem.compound"
    PUBCHEMSUBSTANCE = "pubchem.substance"
    CHEMBL = "chembl"
    RXNORM = "rxcui"
    DRUGBANK = "drugbank"
    WIKIDATA = "wikidata"


class MetaResponse(BaseModel):
    """Metadata for a given source to return in response object."""

    data_license: str
    data_license_url: str
    version: str
    data_url: Optional[str]

    class Config:
        """Configure model"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Customize fields in OpenAPI JSON output"""
            if schema.get('title', None):
                schema.pop('title')
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)


class MatchesKeyed(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an object, not an array.
    """

    match_type: MatchType
    records: List[Drug]
    meta_: MetaResponse

    class Config:
        """Configure model"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Customize fields in OpenAPI JSON output"""
            if schema.get('title', None):
                schema.pop('title')
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)


class MatchesListed(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an array, not an object.
    """

    normalizer: SourceName
    match_type: MatchType
    records: List[Drug]
    meta_: MetaResponse

    class Config:
        """Configure model"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Customize fields in OpenAPI JSON output"""
            if schema.get('title', None):
                schema.pop('title')
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)


class Service(BaseModel):
    """Core response schema containing matches for each source"""

    query: str = Field(..., description="Search string provided by user")
    warnings: Optional[List[str]]
    source_matches: Union[Dict[SourceName, MatchesKeyed], List[MatchesListed]]

    class Config:
        """Configure model"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type['Service']) -> None:
            """Customize fields in OpenAPI JSON output"""
            if schema.get('title', None):
                schema.pop('title')
            for prop in schema.get('properties', {}).values():
                prop.pop('title', None)
