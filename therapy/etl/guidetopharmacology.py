"""Module for Guide to PHARMACOLOGY ETL methods."""
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
import re
import csv
import html

import requests
import bs4

from therapy import logger, PROJECT_ROOT, DownloadException
from therapy.database import Database
from therapy.schemas import SourceMeta, SourceName, NamespacePrefix, \
    ApprovalStatus
from therapy.etl.base import Base


class GuideToPHARMACOLOGY(Base):
    """Class for Guide to PHARMACOLOGY ETL methods."""

    def __init__(self, database: Database,
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

    def _find_version(self) -> str:
        """Find most recent data version.

        :return: Most recent data version
        """
        r = requests.get(self._data_url)
        status_code = r.status_code
        if status_code == 200:
            soup = bs4.BeautifulSoup(r.content, features="lxml")
        else:
            logger.error(f"GuideToPHARMACOLOGY version fetch failed with"
                         f" status code: {status_code}")
            raise DownloadException
        data = soup.find("a", {"name": "data"}).find_next("div").find_next("div").find_next("b")  # noqa: E501
        result = re.search(r"\d{4}.\d+", data.contents[0])  # type: ignore
        return result.group()  # type: ignore

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

    def _transform_data(self) -> None:
        """Transform Guide To PHARMACOLOGY data."""
        data: Dict[str, Any] = dict()
        self._transform_ligands(data)
        self._transform_ligand_id_mappings(data)
        for param in data.values():
            self._load_therapy(param)

    def _transform_ligands(self, data: Dict) -> None:
        """Transform ligands data file and add this data to `data`.

        :param dict data: Transformed data
        """
        with open(self._ligands_file, "r") as f:
            rows = csv.reader(f, delimiter="\t")
            next(rows)

            for row in rows:
                params: Dict[str, Union[List[str], str]] = {
                    "concept_id":
                        f"{NamespacePrefix.GUIDETOPHARMACOLOGY.value}:{row[0]}",  # noqa: E501
                    "label": row[1],
                    "src_name": SourceName.GUIDETOPHARMACOLOGY.value
                }

                approval_status = self._set_approval_status(row[4], row[5])
                if approval_status:
                    params["approval_status"] = approval_status

                associated_with = list()
                aliases = list()
                if row[8]:
                    associated_with.append(f"{NamespacePrefix.PUBCHEMSUBSTANCE.value}:{row[8]}")  # noqa: E501
                if row[9]:
                    associated_with.append(f"{NamespacePrefix.PUBCHEMCOMPOUND.value}:{row[9]}")  # noqa: E501
                if row[10]:
                    associated_with.append(f"{NamespacePrefix.UNIPROT.value}:{row[10]}")  # noqa: E501
                if row[11]:
                    # IUPAC
                    aliases.append(row[11])
                if row[12]:
                    # International Non-proprietary Name assigned by the WHO
                    aliases.append(row[12])
                if row[13]:
                    # synonyms
                    synonyms = row[13].split("|")
                    for s in synonyms:
                        if "&" in s and ";" in s:
                            name_code = s[s.index("&"):s.index(";") + 1]
                            if name_code.lower() in ["&reg;", "&trade;"]:
                                # Remove trademark symbols to allow for search
                                s = s.replace(name_code, "")
                            s = html.unescape(s)
                        aliases.append(s)
                if row[15]:
                    associated_with.append(f"{NamespacePrefix.INCHIKEY.value}:{row[15]}")  # noqa: E501

                if associated_with:
                    params["associated_with"] = associated_with
                if aliases:
                    params["aliases"] = aliases

                data[params["concept_id"]] = params

    def _transform_ligand_id_mappings(self, data: Dict) -> None:
        """Transform ligand_id_mappings and add this data to `data`
        All ligands found in this file should already be in data

        :param dict data: Transformed data
        """
        with open(self._ligand_id_mapping_file.absolute(), "r") as f:
            rows = csv.reader(f, delimiter="\t")
            for row in rows:
                concept_id = f"{NamespacePrefix.GUIDETOPHARMACOLOGY.value}:{row[0]}"  # noqa: E501

                if concept_id not in data:
                    logger.debug(f"{concept_id} not in ligands")
                    continue
                params = data[concept_id]
                xrefs = list()
                associated_with = params.get("associated_with", [])
                if row[6]:
                    xrefs.append(f"{NamespacePrefix.CHEMBL.value}:{row[6]}")
                if row[7]:
                    # CHEBI
                    associated_with.append(row[7])
                if row[11]:
                    xrefs.append(f"{NamespacePrefix.CASREGISTRY.value}:{row[11]}")  # noqa: E501
                if row[12]:
                    xrefs.append(f"{NamespacePrefix.DRUGBANK.value}:{row[12]}")
                if row[13]:
                    associated_with.append(f"{NamespacePrefix.DRUGCENTRAL.value}:{row[13]}")  # noqa: E501

                if xrefs:
                    params["xrefs"] = xrefs
                if associated_with:
                    params["associated_with"] = associated_with

    def _set_approval_status(self, approved: str,
                             withdrawn: str) -> Optional[str]:
        """Set approval status.

        :param str approved: The drug is or has in the past been approved for
            human clinical use by a regulatory agency
        :param str withdrawn: The drug is no longer approved for its original
            clinical use in one or more countries
        :return: Approval status
        """
        if approved and not withdrawn:
            approval_status: Optional[str] = ApprovalStatus.GTOPDB_APPROVED.value
        elif withdrawn:
            approval_status = ApprovalStatus.GTOPDB_WITHDRAWN.value
        else:
            approval_status = None
        return approval_status

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
