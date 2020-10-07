"""A base class for normalizing therapy terms."""
from abc import ABC, abstractmethod
from collections import namedtuple
from enum import IntEnum

IDENTIFIER_PREFIXES = {
    'casRegistry': 'chemidplus',
    'pubchemCompound': 'pubchem.compound',
    'pubchemSubstance': 'pubchem.substance',
    'chembl': 'chembl',
    'rxnorm': 'rxcui',
    'drugbank': 'drugbank'
}


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
    'meta', ['data_license', 'data_license_url']
)


class Base(ABC):
    """The normalizer base class."""

    def __init__(self, *args, **kwargs):
        """Initialize the normalizer."""
        self._load_data(*args, **kwargs)

    @abstractmethod
    def _meta_(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _load_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def normalize(self, query):
        """Normalize query to resource concept"""
        raise NotImplementedError

    NormalizerResponse = namedtuple(
        'NormalizerResponse',
        ['match_type', 'records', 'meta']
    )
