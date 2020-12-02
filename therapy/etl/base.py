"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
from therapy.schemas import NamespacePrefix
from therapy.database import Database

IDENTIFIER_PREFIXES = {
    'casRegistry': NamespacePrefix.CASREGISTRY.value,
    'pubchemCompound': NamespacePrefix.PUBCHEMCOMPOUND.value,
    'pubchemSubstance': NamespacePrefix.PUBCHEMSUBSTANCE.value,
    'chembl': NamespacePrefix.CHEMBL.value,
    'rxnorm': NamespacePrefix.RXNORM.value,
    'drugbank': NamespacePrefix.DRUGBANK.value,
    'wikidata': NamespacePrefix.WIKIDATA.value,
}


class Base(ABC):
    """The ETL base class."""

    def __init__(self, database: Database, *args, **kwargs):
        """Extract from sources."""
        self.database = database
        self._load_data(*args, **kwargs)

    @abstractmethod
    def _extract_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _transform_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _add_meta(self, *args, **kwargs):
        raise NotImplementedError
