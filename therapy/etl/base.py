"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod

IDENTIFIER_PREFIXES = {
    'casRegistry': 'chemidplus',
    'pubchemCompound': 'pubchem.compound',
    'pubchemSubstance': 'pubchem.substance',
    'chembl': 'chembl',
    'rxnorm': 'rxcui',
    'drugbank': 'drugbank'
}


class Base(ABC):
    """The ETL base class."""

    def __init__(self, *args, **kwargs):
        """Extract from sources."""
        self._load_data(*args, **kwargs)

    @abstractmethod
    def _extract_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _transform_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _load_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _add_meta(self, *args, **kwargs):
        raise NotImplementedError
