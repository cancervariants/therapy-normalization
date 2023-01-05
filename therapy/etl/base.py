"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
import ftplib
from pathlib import Path
import logging
from typing import List, Dict, Optional, Callable
import os
import zipfile
import tempfile
import re
import json
from functools import lru_cache

from pydantic import ValidationError
import requests
import bioversions
from disease.query import QueryHandler as DiseaseNormalizer

from therapy import APP_ROOT, ITEM_TYPES, DownloadException
from therapy.schemas import Drug, SourceName
from therapy.database import Database
from therapy.etl.rules import Rules


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)

DEFAULT_DATA_PATH: Path = APP_ROOT / "data"


class Base(ABC):
    """The ETL base class.

    Default methods are declared to provide basic functions for core source
    data-gathering phases (extraction, transformation, loading), as well
    as some common subtasks (getting most recent version, downloading data
    from an FTP server). Classes should expand or reimplement these methods as
    needed.
    """

    def __init__(self, database: Database, data_path: Path = DEFAULT_DATA_PATH) -> None:
        """Extract from sources.

        :param Database database: application database object
        :param Path data_path: path to app data directory
        """
        self._name = self.__class__.__name__
        self.database = database
        self._src_dir: Path = Path(data_path / self._name.lower())
        self._added_ids: List[str] = []
        self._rules = Rules(SourceName(self._name))

    def perform_etl(self, use_existing: bool = False) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.
        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :param bool use_existing: if True, don't try to retrieve latest source data
        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data(use_existing)
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

    def _zip_handler(self, dl_path: Path, outfile_path: Path) -> None:
        """Provide simple callback function to extract the largest file within a given
        zipfile and save it within the appropriate data directory.
        :param Path dl_path: path to temp data file
        :param Path outfile_path: path to save file within
        """
        with zipfile.ZipFile(dl_path, "r") as zip_ref:
            if len(zip_ref.filelist) > 1:
                files = sorted(
                    zip_ref.filelist, key=lambda z: z.file_size, reverse=True
                )
                target = files[0]
            else:
                target = zip_ref.filelist[0]
            target.filename = outfile_path.name
            zip_ref.extract(target, path=outfile_path.parent)
        os.remove(dl_path)

    @staticmethod
    def _http_download(
        url: str,
        outfile_path: Path,
        headers: Optional[Dict] = None,
        handler: Optional[Callable[[Path, Path], None]] = None,
    ) -> None:
        """Perform HTTP download of remote data file.
        :param str url: URL to retrieve file from
        :param Path outfile_path: path to where file should be saved. Must be an actual
            Path instance rather than merely a pathlike string.
        :param Optional[Dict] headers: Any needed HTTP headers to include in request
        :param Optional[Callable[[Path, Path], None]] handler: provide if downloaded
            file requires additional action, e.g. it's a zip file.
        """
        if handler:
            dl_path = Path(tempfile.gettempdir()) / "therapy_dl_tmp"
        else:
            dl_path = outfile_path
        # use stream to avoid saving download completely to memory
        with requests.get(url, stream=True, headers=headers) as r:
            try:
                r.raise_for_status()
            except requests.HTTPError:
                raise DownloadException(
                    f"Failed to download {outfile_path.name} from {url}."
                )
            with open(dl_path, "wb") as h:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        h.write(chunk)
        if handler:
            handler(dl_path, outfile_path)

    def _ftp_download(self, host: str, host_dir: str, host_fn: str) -> None:
        """Download data file from FTP site.
        :param str host: Source's FTP host name
        :param str host_dir: Data directory located on FTP site
        :param str host_fn: Filename on FTP site to be downloaded
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

    def _parse_version(
        self, file_path: Path, pattern: Optional[re.Pattern] = None
    ) -> str:
        """Get version number from provided file path.

        :param Path file_path: path to located source data file
        :param Optional[re.Pattern] pattern: regex pattern to use
        :return: source data version
        :raises: FileNotFoundError if version parsing fails
        """
        if pattern is None:
            pattern = re.compile(type(self).__name__.lower() + r"_(.+)\..+")
        matches = re.match(pattern, file_path.name)
        if matches is None:
            raise FileNotFoundError
        else:
            return matches.groups()[0]

    def _get_existing_files(self) -> List[Path]:
        """Get existing source files from data directory.
        :return: sorted list of file objects
        """
        return list(sorted(self._src_dir.glob(f"{self._name.lower()}_*.*")))

    def _extract_data(self, use_existing: bool = False) -> None:
        """Get source file from data directory.

        This method should ensure the source data directory exists, acquire source data,
        set the source version value, and assign the source file location to
        `self._src_file`. Child classes needing additional functionality (like setting
        up a DB cursor, or managing multiple source files) will need to reimplement
        this method. If `use_existing` is True, the version number will be parsed from
        the existing filename; otherwise, it will be retrieved from the data source,
        and if the local file is out-of-date, the newest version will be acquired.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        self._src_dir.mkdir(exist_ok=True, parents=True)
        src_name = type(self).__name__.lower()
        if use_existing:
            files = self._get_existing_files()
            if len(files) < 1:
                raise FileNotFoundError(f"No source data found for {src_name}")
            self._src_file: Path = files[-1]
            try:
                self._version = self._parse_version(self._src_file)
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Unable to parse version value from {src_name} source data file "
                    f"located at {self._src_file.absolute().as_uri()} -- "
                    "check filename against schema defined in README: "
                    "https://github.com/cancervariants/therapy-normalization#update-sources"  # noqa: E501
                )
        else:
            self._version = self.get_latest_version()
            fglob = f"{src_name}_{self._version}.*"
            latest = list(self._src_dir.glob(fglob))
            if not latest:
                self._download_data()
                latest = list(self._src_dir.glob(fglob))
            assert len(latest) != 0  # probably unnecessary, but just to be safe
            self._src_file = latest[0]

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

    def __init__(self, database: Database, data_path: Path = DEFAULT_DATA_PATH):
        """Initialize source ETL instance.

        :param therapy.database.Database database: application database
        :param Path data_path: path to normalizer data directory
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
