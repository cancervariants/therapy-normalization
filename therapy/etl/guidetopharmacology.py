"""Module for Guide to PHARMACOLOGY ETL methods."""
from typing import Optional, Dict, Any, List, Union, Tuple
import csv
import html
from pathlib import Path
import re

import requests

from therapy import logger
from therapy.schemas import SourceMeta, SourceName, NamespacePrefix, ApprovalRating
from therapy.etl.base import Base


class GuideToPHARMACOLOGY(Base):
    """Class for Guide to PHARMACOLOGY ETL methods."""

    def _download_data(self) -> None:
        """Download the latest version of Guide to PHARMACOLOGY."""
        logger.info("Retrieving source data for Guide to PHARMACOLOGY")
        if not self._ligands_file.exists():
            self._http_download("https://www.guidetopharmacology.org/DATA/ligands.tsv",
                                self._ligands_file)
            assert self._ligands_file.exists()
        if not self._mapping_file.exists():
            self._http_download("https://www.guidetopharmacology.org/DATA/ligand_id_mapping.tsv",  # noqa: E501
                                self._mapping_file)
            assert self._mapping_file.exists()
        logger.info("Successfully retrieved source data for Guide to PHARMACOLOGY")

    def _download_file(self, file_url: str, fn: str) -> None:
        """Download individual data file.

        :param str file_url: Data url for file
        :param str fn: File name
        """
        r = requests.get(file_url)
        if r.status_code == 200:
            prefix = SourceName.GUIDETOPHARMACOLOGY.value.lower()
            path = self._src_dir / f"{prefix}_{fn}_{self._version}.tsv"
            if fn == "ligands":
                self._ligands_file: Path = path
            else:
                self._mapping_file: Path = path
            with open(str(path), "wb") as f:
                f.write(r.content)

    def _extract_data(self, use_existing: bool = False) -> None:
        """Gather GtoPdb source files.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        self._src_dir.mkdir(exist_ok=True, parents=True)
        prefix = SourceName.GUIDETOPHARMACOLOGY.value.lower()
        if use_existing:
            ligands_files = list(sorted(self._src_dir.glob(f"{prefix}_ligands_*.tsv")))
            if len(ligands_files) < 1:
                raise FileNotFoundError("No GtoPdb ligands files found")

            mapping_file: Optional[Tuple] = None
            for ligands_file in ligands_files[::-1]:
                try:
                    version = self._parse_version(
                        ligands_file,
                        re.compile(prefix + r"_ligands_(.+)\.tsv")
                    )
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "Unable to parse GtoPdb version value from ligands file "
                        f"located at {ligands_file.absolute().as_uri()} -- check "
                        "filename against schema defined in README: "
                        "https://github.com/cancervariants/therapy-normalization#update-sources"  # noqa: E501
                    )
                check_mapping_file = self._src_dir / f"{prefix}_ligand_id_mapping_{version}.tsv"  # noqa: E501
                if check_mapping_file.exists():
                    self.version = version
                    self._ligands_file = ligands_file
                    self._mapping_file = check_mapping_file
                    break
            if mapping_file is None:
                raise FileNotFoundError(
                    "Unable to find complete GtoPdb data set with matching version "
                    "values. Check filenames against schema defined in README: "
                    "https://github.com/cancervariants/therapy-normalization#update-sources"  # noqa: E501
                )
        else:
            self._version = self.get_latest_version()
            self._ligands_file = self._src_dir / f"{prefix}_ligands_{self._version}.tsv"
            self._mapping_file = self._src_dir / f"{prefix}_ligand_id_mapping_{self._version}.tsv"  # noqa: E501
            if not (self._ligands_file.exists() and self._mapping_file.exists()):
                self._download_data()
                assert self._ligands_file.exists()
                assert self._mapping_file.exists()

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
                        f"{NamespacePrefix.GUIDETOPHARMACOLOGY.value}:{row[0]}",
                    "label": row[1],
                    "src_name": SourceName.GUIDETOPHARMACOLOGY.value
                }

                approval_rating = self._set_approval_rating(row[4], row[5])
                if approval_rating:
                    params["approval_ratings"] = [approval_rating]

                associated_with = list()
                aliases = list()
                if row[8]:
                    associated_with.append(f"{NamespacePrefix.PUBCHEMSUBSTANCE.value}:{row[8]}")  # noqa: E501
                if row[9]:
                    associated_with.append(f"{NamespacePrefix.PUBCHEMCOMPOUND.value}:{row[9]}")  # noqa: E501
                if row[10]:
                    associated_with.append(f"{NamespacePrefix.UNIPROT.value}:{row[10]}")
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
        with open(self._mapping_file.absolute(), "r") as f:
            rows = csv.reader(f, delimiter="\t")
            for row in rows:
                concept_id = f"{NamespacePrefix.GUIDETOPHARMACOLOGY.value}:{row[0]}"

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
                    xrefs.append(f"{NamespacePrefix.CASREGISTRY.value}:{row[11]}")
                if row[12]:
                    xrefs.append(f"{NamespacePrefix.DRUGBANK.value}:{row[12]}")
                if row[13]:
                    associated_with.append(f"{NamespacePrefix.DRUGCENTRAL.value}:{row[13]}")  # noqa: E501

                if xrefs:
                    params["xrefs"] = xrefs
                if associated_with:
                    params["associated_with"] = associated_with

    def _set_approval_rating(self, approved: str,
                             withdrawn: str) -> Optional[str]:
        """Set approval rating value.

        :param str approved: The drug is or has in the past been approved for human
            clinical use by a regulatory agency
        :param str withdrawn: The drug is no longer approved for its original clinical
            use in one or more countries
        :return: Approval rating
        """
        if approved and not withdrawn:
            approval_rating: Optional[str] = ApprovalRating.GTOPDB_APPROVED.value
        elif withdrawn:
            approval_rating = ApprovalRating.GTOPDB_WITHDRAWN.value
        else:
            approval_rating = None
        return approval_rating

    def _load_meta(self) -> None:
        """Load Guide to PHARMACOLOGY metadata to database."""
        meta = SourceMeta(
            data_license="CC BY-SA 4.0",
            data_license_url="https://creativecommons.org/licenses/by-sa/4.0/",
            version=self._version,
            data_url="https://www.guidetopharmacology.org/download.jsp",
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
