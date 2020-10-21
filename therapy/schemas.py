"""Data models for representing VICC normalized therapy records."""
from typing import List, Optional
from pydantic import BaseModel
from enum import Enum, IntEnum


class Therapy(BaseModel):
    """A procedure or substance used in the treatment of a disease."""

    label: str
    concept_identifier: str
    aliases: List[str]
    other_identifiers: List[str]

    class Config:
        """Enables orm_mode"""

        orm_mode = True


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
    trade_name: Optional[str]
    label: Optional[str]

    class Config:
        """Enables orm_mode"""

        orm_mode = True


class DrugGroup(Therapy):
    """A grouping of drugs based on common pharmacological attributes."""

    description: str
    type_identifier: str
    drugs: List[Drug]


class MatchType(IntEnum):
    """Define string constaints for use in Match Type attributes"""

    PRIMARY = 100
    NAMESPACE_CASE_INSENSITIVE = 95
    CASE_INSENSITIVE_PRIMARY = 80
    ALIAS = 60
    CASE_INSENSITIVE_ALIAS = 40
    FUZZY_MATCH = 20
    NO_MATCH = 0


class SourceName(Enum):
    """Define string constraints to ensure consistent capitalization"""

    WIKIDATA = "Wikidata"
    CHEMBL = "ChEMBL"
    DRUGBANK = "Drugbank"


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs"""

    CASREGISTRY = "chemidplus"
    PUBCHEMCOMPOUND = "pubchem.compound"
    PUBCHEMSUBSTANCE = "pubchem.substance"
    CHEMBL = "chembl"
    RXNORM = "rxcui"
    DRUGBANK = "drug"
    WIKIDATA = "wikidata"


class TherapyLoad(BaseModel):
    """An entry into the therapies table"""

    concept_id: str
    label: Optional[str]
    max_phase: Optional[PhaseEnum]
    withdrawn: Optional[bool]
    trade_name: Optional[List[str]]


class MetaResponse(BaseModel):
    """Metadata for a given source to return in response object"""

    data_license: str
    data_license_url: str
    version: str
    data_url: str
