"""Data models for representing VICC normalized therapy records."""
from dataclasses import dataclass
from typing import List


@dataclass
class Therapy:
    """A procedure or substance used in the treatment of a disease."""

    name: str
    concept_identifier: str
    aliases: List[str]


@dataclass
class Attribute:
    """Wikidata statement describing an item."""

    property: str
    property_identifier: str
    value: str
    value_identifier: str


@dataclass
class Drug(Therapy):
    """A pharmacologic substance used to treat a medical condition."""

    attributes: List[Attribute]


@dataclass
class DrugGroup(Therapy):
    """A grouping of drugs based on shared common attributes."""

    description: str
    type_identifier: str
    drugs: List[Drug]
