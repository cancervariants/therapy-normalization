"""Module for Guide to PHARMACOLOGY ETL methods."""
from typing import Optional, Dict, Any, List, Union
import csv
import html

import requests

from therapy import logger
from therapy.schemas import SourceMeta, SourceName, NamespacePrefix, \
    ApprovalStatus
from therapy.etl.base import Base


class GuideToPHARMACOLOGY(Base):
    """Class for Guide to PHARMACOLOGY ETL methods."""

    _ligands_data_url: str = "https://www.guidetopharmacology.org/DATA/ligands.tsv"  # noqa: E501
    _ligand_mapping_data_url: str = "https://www.guidetopharmacology.org/DATA/ligand_id_mapping.tsv"  # noqa: E501

    def _download_data(self) -> None:
        """Download the latest version of Guide to PHARMACOLOGY."""
        logger.info("Retrieving source data for Guide to PHARMACOLOGY")
        if not self._src_ligands_file.exists():
            self._http_download(self._ligands_data_url, self._src_ligands_file)
            assert self._src_ligands_file.exists()
        if not self._src_mappings_file.exists():
            self._http_download(self._ligand_mapping_data_url,
                                self._src_mappings_file)
            assert self._src_mappings_file.exists()
        logger.info("Successfully retrieved source data for Guide to PHARMACOLOGY")  # noqa: E501

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
                self._ligands_file = path
            else:
                self._ligand_id_mapping_file = path
            with open(str(path), "wb") as f:
                f.write(r.content)

    def _extract_data(self) -> None:
        """Gather GtoPdb source files."""
        self._src_dir.mkdir(exist_ok=True, parents=True)
        self._version = self.get_latest_version()
        prefix = SourceName.GUIDETOPHARMACOLOGY.value.lower()
        self._src_ligands_file = self._src_dir / f"{prefix}_ligands_{self._version}.tsv"  # noqa: E501
        self._src_mappings_file = self._src_dir / f"{prefix}_ligand_id_mapping_{self._version}.tsv"  # noqa: E501
        if not (self._src_ligands_file.exists()
                and self._src_mappings_file.exists()):
            self._download_data()
            assert self._src_ligands_file.exists()
            assert self._src_mappings_file.exists()

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
            approval_status: Optional[str] = ApprovalStatus.APPROVED.value
        elif withdrawn:
            approval_status = ApprovalStatus.WITHDRAWN.value
        else:
            approval_status = None
        return approval_status

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
