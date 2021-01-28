"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
from therapy.schemas import NamespacePrefix
from therapy.database import Database
from typing import List

# prefixes for translating ID namespaces
IDENTIFIER_PREFIXES = {
    'casRegistry': NamespacePrefix.CASREGISTRY.value,
    'ChemIDplus': NamespacePrefix.CHEMIDPLUS.value,  # needed for wikidata
    'pubchemCompound': NamespacePrefix.PUBCHEMCOMPOUND.value,
    'pubchemSubstance': NamespacePrefix.PUBCHEMSUBSTANCE.value,
    'chembl': NamespacePrefix.CHEMBL.value,
    'rxnorm': NamespacePrefix.RXNORM.value,
    'drugbank': NamespacePrefix.DRUGBANK.value,
    'wikidata': NamespacePrefix.WIKIDATA.value,
}


class Base(ABC):
    """The ETL base class."""

    def __init__(self, database: Database):
        """Extract from sources."""
        self.database = database
        self._added_ids = []

    @abstractmethod
    def perform_etl(self) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.

        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        raise NotImplementedError

    @abstractmethod
    def _extract_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _transform_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _load_meta(self, *args, **kwargs):
        raise NotImplementedError
