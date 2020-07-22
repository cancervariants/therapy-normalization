"""Data models for representing VICC normalized therapy records."""
from typing import List
from pydantic import BaseModel


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


class Drug(Therapy):
    """A pharmacologic substance used to treat a medical condition."""

    fda_approved: bool = False


class DrugGroup(Therapy):
    """A grouping of drugs based on common pharmacological attributes."""

    description: str
    type_identifier: str
    drugs: List[Drug]
