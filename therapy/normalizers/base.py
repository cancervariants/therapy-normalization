"""A base class for normalizing therapy terms."""
from abc import ABC, abstractmethod
from collections import namedtuple
from enum import IntEnum
import logging
import re
from uvicorn.config import logger

logging.basicConfig(filename='therapy.log', level=logging.DEBUG)
logger = logging.getLogger('therapy') # noqa
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(ch)

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


class Base(ABC):
    """The normalizer base class."""

    def __init__(self, *args, **kwargs):
        """Initialize the normalizer."""
        self._load_data(*args, **kwargs)

    @abstractmethod
    def _load_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def normalize(self, query):
        """Normalize query to resource concept"""
        raise NotImplementedError

    def _white_space_sanitization(self, query):
        file = open("therapy.log", "w+")
        file.truncate(0)
        file.close()

        nbsp = re.search('\xa0|\u00A0|&nbsp;', query)
        if nbsp:
            logger.warning(
                f'Query ({query}) contains non breaking space characters.'
            )

    NormalizerResponse = namedtuple(
        'NormalizerResponse',
        ['match_type', 'records']
    )
