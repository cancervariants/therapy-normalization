"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
from ftplib import FTP
from pathlib import Path
from typing import List, Dict
import logging

from pydantic import ValidationError

from therapy import PROJECT_ROOT, ITEM_TYPES
from therapy.schemas import Drug
from therapy.database import Database


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class Base(ABC):
    """The ETL base class."""

    def __init__(self, database: Database,
                 data_path: Path = PROJECT_ROOT / "data"):
        """Extract from sources.

        :param Database database: application database object
        :param Path data_path: path to app data directory
        """
        name = self.__class__.__name__.lower()
        self.database: Database = database
        self._src_data_dir: Path = Path(data_path / name)
        self._added_ids: List[str] = []

    def perform_etl(self) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.

        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data()
        self._load_meta()
        self._transform_data()
        return self._added_ids

    def _download_data(self) -> None:
        raise NotImplementedError

    def _ftp_download(self, host: str, data_dir: str, source_dir: Path,
                      data_fn: str) -> None:
        """Download data file from FTP site.
        :param str host: Source"s FTP host name
        :param str data_dir: Data directory located on FTP site
        :param Path source_dir: Source"s data directory
        :param str data_fn: Filename on FTP site to be downloaded
        """
        try:
            with FTP(host) as ftp:
                ftp.login()
                logger.debug(f"FTP login to {host} was successful")
                ftp.cwd(data_dir)
                with open(source_dir / data_fn, "wb") as fp:
                    ftp.retrbinary(f"RETR {data_fn}", fp.write)
        except Exception as e:
            logger.error(f"FTP download failed: {e}")
            raise Exception(e)

    def _extract_data(self) -> None:
        """Get source file from data directory."""
        self._src_data_dir.mkdir(exist_ok=True, parents=True)
        src_file_prefix = f"{type(self).__name__.lower()}_"
        dir_files = [f for f in self._src_data_dir.iterdir()
                     if f.name.startswith(src_file_prefix)]
        if len(dir_files) == 0:
            self._download_data()
            dir_files = [f for f in self._src_data_dir.iterdir()
                         if f.name.startswith(src_file_prefix)]
        self._src_file = sorted(dir_files, reverse=True)[0]
        self._version = self._src_file.stem.split("_", 1)[1]

    @abstractmethod
    def _transform_data(self) -> None:
        raise NotImplementedError

    def _load_therapy(self, therapy: Dict) -> None:
        """Load individual therapy record into database.
        This method takes responsibility for:
            * validating record structure correctness
            * removing duplicates from list-like fields
            * removing empty fields

        :param Dict therapy: valid therapy object.
        """
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

                if isinstance(value, str):
                    self.database.add_ref_record(value.lower(),
                                                 concept_id, item_type)
                    continue

                if "label" in therapy:
                    try:
                        value.remove(therapy["label"])
                    except ValueError:
                        pass

                if attr_type == "aliases" and "trade_names" in therapy:
                    value = list(set(value) - set(therapy["trade_names"]))

                if len(value) > 20:
                    logger.debug(f"{concept_id} has > 20 {attr_type}.")
                    del therapy[attr_type]
                    continue
                for item in {item.lower() for item in value}:
                    self.database.add_ref_record(item, concept_id, item_type)
        assert Drug(**therapy)
        concept_id = therapy["concept_id"]

        # handle detail fields
        approval_attrs = ("approval_status", "approval_year", "has_indication")
        for field in approval_attrs:
            if approval_attrs in therapy and therapy[field] is None:
                del therapy[field]

        self.database.add_record(therapy)
        self._added_ids.append(concept_id)

    @abstractmethod
    def _load_meta(self) -> None:
        raise NotImplementedError
