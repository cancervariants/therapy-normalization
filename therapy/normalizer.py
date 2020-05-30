"""Methods to normalize therapy terms."""
from abc import ABC, abstractmethod
from therapy import PROJECT_ROOT
import json


class Base(ABC):
    """The normalizer base class."""

    def __init__(self, *args, **kwargs):
        """Initialize the normalizer."""
        self._data = None
        self._load_data(*args, **kwargs)

    @abstractmethod
    def _load_data(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def normalize(self, term):
        """Normalize term to wikidata concept"""
        raise NotImplementedError


class Wikidata(Base):
    """A normalizer using the Wikidata resource."""

    def normalize(self, term):
        """Normalize term using Wikidata"""
        return None

    def _load_data(self, *args, **kwargs):
        with open(
            PROJECT_ROOT / 'data' / 'wikidata_medications.json', 'r'
        ) as f:
            self._data = json.load(f)
