"""A base class for extraction, transformation, and loading of data."""
import json
import logging
from abc import ABC, abstractmethod
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

import click
from disease.database.dynamodb import DynamoDbDatabase
from disease.query import QueryHandler as DiseaseNormalizer
from pydantic import ValidationError
from wags_tails import (
    ChemblData,
    ChemIDplusData,
    CustomData,
    DataSource,
    DrugBankData,
    DrugsAtFdaData,
    GToPLigandData,
    HemOncData,
    NcitData,
    RxNormData,
)

from therapy import ITEM_TYPES
from therapy.database import Database
from therapy.etl.rules import Rules
from therapy.schemas import Drug, SourceName

logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


DATA_DISPATCH = {
    SourceName.CHEMBL: ChemblData,
    SourceName.CHEMIDPLUS: ChemIDplusData,
    SourceName.DRUGBANK: DrugBankData,
    SourceName.DRUGSATFDA: DrugsAtFdaData,
    SourceName.GUIDETOPHARMACOLOGY: GToPLigandData,
    SourceName.HEMONC: HemOncData,
    SourceName.NCIT: NcitData,
    SourceName.RXNORM: RxNormData,
}


class Base(ABC):
    """The ETL base class.

    Default methods are declared to provide basic functions for core source
    data-gathering phases (extraction, transformation, loading), as well
    as some common subtasks (getting most recent version, downloading data
    from an FTP server). Classes should expand or reimplement these methods as
    needed.
    """

    def __init__(
        self, database: Database, data_path: Optional[Path] = None, silent: bool = True
    ) -> None:
        """Extract from sources.

        :param database: application database object
        :param data_path: path to app data directory
        :param silent: if True, don't print ETL results to console
        """
        self._silent = silent
        self._src_name = SourceName(self.__class__.__name__)
        self._data_source: Union[
            ChemblData,
            ChemIDplusData,
            DrugBankData,
            DrugsAtFdaData,
            GToPLigandData,
            HemOncData,
            NcitData,
            RxNormData,
            CustomData,
        ] = self._get_data_handler(data_path)  # type: ignore
        self.database = database
        self._added_ids: List[str] = []
        self._rules = Rules(self._src_name)

    def _get_data_handler(self, data_path: Optional[Path] = None) -> DataSource:
        """Construct data handler instance for source. Overwrite for edge-case sources.

        :param data_path: location of data storage
        :return: instance of wags_tails.DataSource to manage source file(s)
        """
        return DATA_DISPATCH[self._src_name](data_dir=data_path, silent=self._silent)

    def perform_etl(self, use_existing: bool = False) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.
        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :param use_existing: if True, don't try to retrieve latest source data
        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data(use_existing)
        if not self._silent:
            click.echo("Transforming and loading data to DB...")
        self._load_meta()
        self._transform_data()
        return self._added_ids

    def _extract_data(self, use_existing: bool) -> None:
        """Acquire source data.

        This method is responsible for initializing an instance of a data handler and,
        in most cases, setting ``self._data_file`` and ``self._version``.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        self._data_file, self._version = self._data_source.get_latest(
            from_local=use_existing
        )

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

    def __init__(
        self, database: Database, data_path: Optional[Path] = None, silent: bool = True
    ) -> None:
        """Extract from sources.

        :param database: application database object
        :param data_path: path to app data directory
        :param silent: if True, don't print ETL results to console
        """
        super().__init__(database, data_path, silent)
        db = DynamoDbDatabase(self.database.endpoint_url)
        self.disease_normalizer = DiseaseNormalizer(db)

    @lru_cache(maxsize=64)
    def _normalize_disease(self, query: str) -> Optional[str]:
        """Attempt normalization of disease term.
        :param str query: term to normalize
        :return: ID if successful, None otherwise
        """
        response = self.disease_normalizer.normalize(query)
        if response.match_type > 0:
            return response.normalized_id
        else:
            logger.warning(f"Failed to normalize disease term: {query}")
            return None


class SourceFormatException(Exception):  # noqa: N818
    """Raise when source data formatting is incompatible with the source transformation
    methods: for example, if columns in a CSV file have changed.
    """
