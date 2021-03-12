"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
from typing import List


class Base(ABC):
    """The ETL base class."""

    def __init__(self, database):
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
