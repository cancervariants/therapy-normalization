"""Module for Guide to PHARMACOLOGY ETL methods."""
from therapy import logger, PROJECT_ROOT, DownloadException
from therapy.etl.base import Base
from therapy.schemas import SourceMeta, SourceName
from pathlib import Path
import requests
import bs4
import re


class GuideToPHARMACOLOGY(Base):
    """Class for Guide to PHARMACOLOGY ETL methods."""

    def __init__(self, database,
                 data_path: Path = PROJECT_ROOT / "data") -> None:
        """Initialize GuideToPHARMACOLOGY ETL class.

        :param therapy.database.Database: DB instance to use
        :param Path data_path: path to app data directory
        """
        super().__init__(database, data_path)
        self._data_url = "https://www.guidetopharmacology.org/download.jsp"
        self._version = self._find_version()
        self._ligands_data_url = "https://www.guidetopharmacology.org/DATA/ligands.tsv"  # noqa: E501
        self._ligand_id_mapping_data_url = "https://www.guidetopharmacology.org/DATA/ligand_id_mapping.tsv"  # noqa: E501
        self._ligands_file = None
        self._ligand_id_mapping_file = None

    def _find_version(self) -> str:
        """Find most recent data version.

        :return: Most recent data version
        """
        r = requests.get(self._data_url)
        status_code = r.status_code
        if status_code == 200:
            soup = bs4.BeautifulSoup(r.content, features='lxml')
        else:
            logger.error(f"GuideToPHARMACOLOGY version fetch failed with"
                         f" status code: {status_code}")
            raise DownloadException
        data = soup.find("a", {"name": "data"}).find_next("div").find_next("div").find_next("b")  # noqa: E501
        return re.search(r"\d{4}.\d+", data.contents[0]).group()

    def _extract_data(self) -> None:
        """Extract data from Guide to PHARMACOLOGY."""
        logger.info("Extracting Guide to PHARMACOLOGY data...")
        self._src_data_dir.mkdir(exist_ok=True, parents=True)
        self._download_data()
        logger.info("Successfully extracted Guide to PHARMACOLOGY data.")

    def _download_data(self) -> None:
        """Download the latest version of Guide to PHARMACOLOGY."""
        logger.info("Downloading Guide to PHARMACOLOGY data...")
        dir_files = list(self._src_data_dir.iterdir())
        if len(dir_files) > 0:
            prefix = SourceName.GUIDETOPHARMACOLOGY.value.lower()
            for f in dir_files:
                if f.name == f"{prefix}_ligands_{self._version}.tsv":
                    self._ligands_file = f
                elif f.name == f"{prefix}_ligand_id_mapping_{self._version}.tsv":  # noqa: E501
                    self._ligand_id_mapping_file = f

        if self._ligands_file is None:
            self._download_file(self._ligands_data_url, "ligands")
        if self._ligand_id_mapping_file is None:
            self._download_file(self._ligand_id_mapping_data_url,
                                "ligand_id_mapping")
        logger.info("Successfully downloaded Guide to PHARMACOLOGY data.")

    def _download_file(self, file_url: str, fn: str) -> None:
        """Download individual data file.

        :param str file_url: Data url for file
        :param str fn: File name
        """
        r = requests.get(file_url)
        if r.status_code == 200:
            prefix = SourceName.GUIDETOPHARMACOLOGY.value.lower()
            path = self._src_data_dir / f"{prefix}_{fn}_{self._version}.tsv"
            if fn == "ligands":
                self._ligands_file = path
            else:
                self._ligand_id_mapping_file = path
            with open(str(path), "wb") as f:
                f.write(r.content)

    def _transform_data(self):
        pass

    def _load_meta(self) -> None:
        """Load Guide to PHARMACOLOGY metadata to database."""
        meta = SourceMeta(
            data_license="CC BY-SA 4.0",
            data_license_url="https://creativecommons.org/licenses/by-sa/4.0/",
            version=self._version,
            data_url=self._data_url,
            rdp_url=None,
            data_license_attributes={
                "non_commercial": False,
                "share_alike": True,
                "attribution": True,
            }
        )
        params = dict(meta)
        params["src_name"] = SourceName.GUIDETOPHARMACOLOGY.value
        self.database.metadata.put_item(Item=params)
