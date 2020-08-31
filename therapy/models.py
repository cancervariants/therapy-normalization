"""Data models for representing VICC normalized therapy records."""
from typing import List, Optional
from pydantic import BaseModel
from enum import IntEnum


class Therapy(BaseModel):
    """A procedure or substance used in the treatment of a disease."""

    label: str
    concept_identifier: str
    aliases: List[str]
    other_identifiers: List[str]


# class Attribute(BaseModel):
#     """Wikidata statement describing an item."""
#
#     property: str
#     property_identifier: str
#     value: str
#     value_identifier: str


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


class DrugGroup(Therapy):
    """A grouping of drugs based on common pharmacological attributes."""

    description: str
    type_identifier: str
    drugs: List[Drug]
