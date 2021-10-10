"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
import ftplib
from pathlib import Path
import logging
from typing import List, Dict, Union, Set

import requests
import bioversions

from therapy import APP_ROOT, ITEM_TYPES, DownloadException
from therapy.database import Database
from therapy.schemas import Drug


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)

DEFAULT_DATA_PATH = APP_ROOT / "data"


class Base(ABC):
    """The ETL base class.

    Default methods are declared to provide basic functions for core source
    data-gathering phases (extraction, transformation, loading), as well
    as some common subtasks (getting most recent version, downloading data
    from an FTP server). Classes should expand or reimplement these methods as
    needed.

    TODO
    * base http method
    """

    def __init__(self, database: Database,
                 data_path: Path = DEFAULT_DATA_PATH) -> None:
        """Extract from sources.

        :param Database database: application database object
        :param Path data_path: path to app data directory
        """
        self.database = database
        self._src_dir: Path = data_path / self.__class__.__name__.lower()
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

    def get_latest_version(self) -> str:
        """Get most recent version of source data. Should be overriden by
        sources not added to Bioversions yet, or other special-case sources.
        :return: most recent version, as a str
        """
        return bioversions.get_version(self.__class__.__name__)

    @abstractmethod
    def _download_data(self) -> None:
        """Acquire source data and deposit in a usable form with correct file
        naming conventions (generally, `<source>_<version>.<filetype>`, or
        `<source>_<subset>_<version>.<filetype>` if sources require multiple
        files). Shouldn't set any instance attributes.
        """
        raise NotImplementedError

    @staticmethod
    def _http_download(url: str, fname: Path) -> None:
        """Perform HTTP download of remote data file.
        :param str url: URL to retrieve file from
        :param Path fname: path to where file should be saved
        """
        r = requests.get(url)
        if r.status_code != 200:
            raise DownloadException(f"Failed to download {fname.name} from "
                                    f"{url}.")
        with open(fname, "wb") as f:
            f.write(r.content)

    def _ftp_download(self, host: str, host_dir: str, host_fn: str) -> None:
        """Download data file from FTP site.
        :param str host: Source's FTP host name
        :param str host_dir: Data directory located on FTP site
        :param str host_fn: Filename on FTP site to be downloaded

        #TODO ALSO HTTP DOWNLOAD METHOD
        """
        try:
            with ftplib.FTP(host) as ftp:
                ftp.login()
                logger.debug(f"FTP login to {host} was successful")
                ftp.cwd(host_dir)
                with open(self._src_dir / host_fn, "wb") as fp:
                    ftp.retrbinary(f"RETR {host_fn}", fp.write)
        except ftplib.all_errors as e:
            logger.error(f"FTP download failed: {e}")
            raise Exception(e)

    def _extract_data(self) -> None:
        """Get source file from data directory.
        This method should create the source data directory if needed,
        acquire the most recent version number, check that local data is
        up-to-date and retrieve the latest data if needed, and set the
        `self._src_file` attribute to the source file location. Child classes
        could add additional functions, e.g. setting up DB cursors.

        Sources that use multiple data files (such as RxNorm and HemOnc) will
        have to reimplement this method.
        """
        self._src_dir.mkdir(exist_ok=True, parents=True)
        self._version = self.get_latest_version()
        fglob = f"{type(self).__name__.lower()}_{self._version}.*"
        latest = list(self._src_dir.glob(fglob))
        if not latest:
            self._download_data()
            latest = list(self._src_dir.glob(fglob))
        # TODO shouldnt be glob -- should just expect exact filename
        self._src_file: Path = latest[0]

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
        """Load individual therapy record. Subclasses should not be overriding
        this implementation.

        :param Dict therapy: valid therapy object.
        """
        assert Drug(**therapy)
        concept_id = therapy["concept_id"]

        for attr_type, item_type in ITEM_TYPES.items():
            if attr_type in therapy:
                value = therapy[attr_type]
                if value is not None and value != []:
                    if isinstance(value, str):
                        items: Union[List, Set] = [value.lower()]
                    else:
                        therapy[attr_type] = list(set(value))
                        items = {item.lower() for item in value}
                        if attr_type in ["aliases", "trade_names"]:
                            # remove duplicates
                            if "label" in therapy:
                                therapy[attr_type] = list(set(therapy[attr_type]) - {therapy["label"]})  # noqa: E501

                            if attr_type == "aliases" and \
                                    "trade_names" in therapy:
                                therapy[attr_type] = list(set(therapy[attr_type]) - set(therapy["trade_names"]))  # noqa: E501

                            if len(items) > 20:
                                logger.debug(f"{concept_id} has > 20"
                                             f" {attr_type}.")
                                del therapy[attr_type]
                                continue

                    for item in items:
                        self.database.add_ref_record(item, concept_id,
                                                     item_type)
                else:
                    del therapy[attr_type]

        # handle detail fields
        approval_attrs = ("approval_status", "approval_year", "fda_indication")
        for field in approval_attrs:
            if approval_attrs in therapy and therapy[field] is None:
                del therapy[field]

        self.database.add_record(therapy)
        self._added_ids.append(concept_id)
