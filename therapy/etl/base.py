"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
from pathlib import Path
import logging
from typing import List, Dict, Optional, Type
import json
from functools import lru_cache

from pydantic import ValidationError
from disease.query import QueryHandler as DiseaseNormalizer
from wags_tails.base_source import DataSource

from therapy import ITEM_TYPES
from therapy.schemas import Drug, SourceName
from therapy.database import Database
from therapy.etl.rules import Rules


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class Base(ABC):
    """The ETL base class.

    Default methods are declared to provide basic functions for core source
    data-gathering phases (extraction, transformation, loading), as well
    as some common subtasks (getting most recent version, downloading data
    from an FTP server). Classes should expand or reimplement these methods as
    needed.
    """

    _DataSourceClass: Type[DataSource]

    def __init__(self, database: Database, data_path: Optional[Path] = None) -> None:
        """Extract from sources.

        :param database: application database object
        :param data_path: preferred Thera-Py data directory location, if necessary
        """
        self._therapy_data_dir = data_path
        self._name = self.__class__.__name__
        self.database = database
        self._added_ids: List[str] = []
        self._rules = Rules(SourceName(self._name))

    def perform_etl(self, use_existing: bool = False) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.
        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :param use_existing: if True, don't try to retrieve latest source data
        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data(use_existing)
        self._load_meta()
        self._transform_data()
        return self._added_ids

    def _extract_data(self, use_existing: bool) -> None:
        """Acquire source data.

        This method is responsible for initializing an instance of
        ``self._DataSourceClass``, and, in most cases, setting ``self._src_file``.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        data_source = self._DataSourceClass(data_dir=self._therapy_data_dir)
        self._src_file, self._version = data_source.get_latest(from_local=use_existing)

    @abstractmethod
    def _load_meta(self) -> None:
        """Load source metadata entry."""
        raise NotImplementedError

    @abstractmethod
    def _transform_data(self) -> None:
        """Prepare source data for loading into DB. Individually extract each
        record and call the Base class's `_load_therapy()` method.
        """
        raise NotImplementedError

    def _load_therapy(self, therapy: Dict) -> None:
        """Load individual therapy record into database.
        Additionally, this method takes responsibility for:
            * validating record structure correctness
            * removing duplicates from list-like fields
            * removing empty fields

        :param Dict therapy: valid therapy object.
        """
        therapy = self._rules.apply_rules_to_therapy(therapy)
        try:
            Drug(**therapy)
        except ValidationError as e:
            logger.error(f"Attempted to load invalid therapy: {therapy}")
            raise e

        concept_id = therapy["concept_id"]

        for attr_type, item_type in ITEM_TYPES.items():
            if attr_type in therapy:
                value = therapy[attr_type]
                if value is None or value == []:
                    del therapy[attr_type]
                    continue

                if attr_type == "label":
                    value = value.strip()
                    therapy["label"] = value
                    self.database.add_ref_record(value.lower(), concept_id, item_type)
                    continue

                value_set = {v.strip() for v in value}

                # clean up listlike symbol fields
                if attr_type == "aliases" and "trade_names" in therapy:
                    value = list(value_set - set(therapy["trade_names"]))
                else:
                    value = list(value_set)

                if attr_type in ("aliases", "trade_names"):
                    if "label" in therapy:
                        try:
                            value.remove(therapy["label"])
                        except ValueError:
                            pass

                if len(value) > 20:
                    logger.debug(f"{concept_id} has > 20 {attr_type}.")
                    del therapy[attr_type]
                    continue

                for item in {item.lower() for item in value}:
                    self.database.add_ref_record(item, concept_id, item_type)
                therapy[attr_type] = value

        # compress has_indication
        indications = therapy.get("has_indication")
        if indications:
            therapy["has_indication"] = list(
                {
                    json.dumps(
                        [
                            ind["disease_id"],
                            ind["disease_label"],
                            ind.get("normalized_disease_id"),
                            ind.get("supplemental_info"),
                        ]
                    )
                    for ind in indications
                }
            )
        elif "has_indication" in therapy:
            del therapy["has_indication"]

        # handle detail fields
        approval_attrs = ("approval_ratings", "approval_year")
        for field in approval_attrs:
            if approval_attrs in therapy and therapy[field] is None:
                del therapy[field]

        self.database.add_record(therapy)
        self._added_ids.append(concept_id)


class DiseaseIndicationBase(Base):
    """Base class for sources that require disease normalization capabilities."""

    def __init__(self, database: Database, data_path: Optional[Path] = None):
        """Initialize source ETL instance.

        :param database: application database
        :param data_path: preferred Thera-Py data directory location, if necessary
        """
        super().__init__(database, data_path)
        self.disease_normalizer = DiseaseNormalizer(self.database.endpoint_url)

    @lru_cache(maxsize=64)
    def _normalize_disease(self, query: str) -> Optional[str]:
        """Attempt normalization of disease term.
        :param str query: term to normalize
        :return: ID if successful, None otherwise
        """
        response = self.disease_normalizer.normalize(query)
        if response.match_type > 0:
            return response.disease_descriptor.disease
        else:
            logger.warning(f"Failed to normalize disease term: {query}")
            return None


class SourceFormatException(Exception):
    """Raise when source data formatting is incompatible with the source transformation
    methods: for example, if columns in a CSV file have changed.
    """

    pass
