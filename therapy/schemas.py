"""Data models for representing VICC normalized therapy records."""
from typing import List, Optional
from pydantic import BaseModel
from enum import IntEnum
from collections import namedtuple


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
    """Define string constants for use in Match Type attributes"""

    PRIMARY = 100
    NAMESPACE_CASE_INSENSITIVE = 95
    CASE_INSENSITIVE_PRIMARY = 80
    ALIAS = 60
    CASE_INSENSITIVE_ALIAS = 40
    FUZZY_MATCH = 20
    NO_MATCH = 0


Meta = namedtuple(
    'Meta', ['data_license', 'data_license_url', 'version', 'data_url']
)
