"""Provide ETL methods for HemOnc.org data."""
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional
import csv
import os
import zipfile
import re

import requests
import isodate

from therapy import DownloadException
from therapy.schemas import NamespacePrefix, SourceMeta, SourceName, RecordParams, \
    ApprovalRating
from therapy.etl.base import DiseaseIndicationBase


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class HemOnc(DiseaseIndicationBase):
    """Class for HemOnc.org ETL methods."""

    def get_latest_version(self) -> str:
        """Retrieve latest version of source data.
        :raise: Exception if retrieval is unsuccessful
        """
        response = requests.get("https://dataverse.harvard.edu/api/datasets/export?persistentId=doi:10.7910/DVN/9CY9C6&exporter=dataverse_json")  # noqa: E501
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error("Unable to retrieve HemOnc version from Harvard Dataverse")
            raise e
        iso_datetime = isodate.parse_datetime(
            response.json()["datasetVersion"]["releaseTime"]
        )
        return iso_datetime.strftime(isodate.isostrf.DATE_EXT_COMPLETE)

    def _zip_handler(self, dl_path: Path, outfile_path: Path) -> None:
        """Extract concepts, rels, and synonyms files from tmp zip file and save to
        data directory.
        :param Path dl_path: path to temp data zipfile
        :param Path outfile_path: directory to save data within
        """
        file_terms = ("concepts", "rels", "synonyms")
        with zipfile.ZipFile(dl_path, "r") as zip_ref:
            for file in zip_ref.filelist:
                for term in file_terms:
                    if term in file.filename:
                        file.filename = f"hemonc_{term}_{self._version}.csv"
                        zip_ref.extract(file, outfile_path)

        os.remove(dl_path)

    def _download_data(self) -> None:
        """Download HemOnc.org source data. Requires Harvard Dataverse API key to be set
        as environment variable DATAVERSE_API_KEY. Instructions for generating an API
        key are available here: https://guides.dataverse.org/en/latest/user/account.html

        :raises: DownloadException if API key environment variable isn't set
        """
        api_key = os.environ.get("DATAVERSE_API_KEY")
        if api_key is None:
            raise DownloadException(
                "Must provide Harvard Dataverse API key in environment variable "
                "DATAVERSE_API_KEY. See "
                "https://guides.dataverse.org/en/latest/user/account.html"
            )
        url = "https://dataverse.harvard.edu//api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/9CY9C6"  # noqa: E501
        headers = {"X-Dataverse-key": api_key}
        self._http_download(url, self._src_dir, headers, self._zip_handler)

    def _extract_data(self, use_existing: bool = False) -> None:
        """Get source files from data directory.

        The following files are necessary for data processing:
            hemonc_concepts_<version>.csv
            hemonc_rels_<version>.csv
            hemonc_synonyms_<version>.csv
        This method will attempt to retrieve their latest versions if they are
        unavailable locally.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        self._src_dir.mkdir(exist_ok=True, parents=True)

        if use_existing:
            concepts = list(sorted(self._src_dir.glob("hemonc_concepts_*.csv")))
            if len(concepts) < 1:
                raise FileNotFoundError("No HemOnc concepts file found")

            src_files: Optional[Tuple] = None
            for concepts_file in concepts[::-1]:
                try:
                    version = self._parse_version(
                        concepts_file,
                        re.compile(r"hemonc_concepts_(.+)\.csv")
                    )
                except FileNotFoundError:
                    raise FileNotFoundError(
                        f"Unable to parse HemOnc version value from concepts file "
                        f"located at {concepts_file.absolute().as_uri()} -- check "
                        "filename against schema defined in README: "
                        "https://github.com/cancervariants/therapy-normalization#update-sources"  # noqa: E501
                    )
                other_files = (
                    self._src_dir / f"hemonc_rels_{version}.csv",
                    self._src_dir / f"hemonc_synonyms_{version}.csv"
                )
                if other_files[0].exists() and other_files[1].exists():
                    self.version = version
                    src_files = (
                        concepts_file,
                        other_files[0],
                        other_files[1]
                    )
                    break
            if src_files is None:
                raise FileNotFoundError(
                    "Unable to find complete HemOnc data set with matching version "
                    "values. Check filenames against schema defined in README: "
                    "https://github.com/cancervariants/therapy-normalization#update-sources"  # noqa: E501
                )
            else:
                self._src_files = src_files
        else:
            self._version = self.get_latest_version()
            data_filenames = (
                self._src_dir / f"hemonc_concepts_{self._version}.csv",
                self._src_dir / f"hemonc_rels_{self._version}.csv",
                self._src_dir / f"hemonc_synonyms_{self._version}.csv"
            )
            if not all((f.exists() for f in data_filenames)):
                self._download_data()
            self._src_files = data_filenames
            for file in self._src_files:
                assert file.exists()

    def _load_meta(self) -> None:
        """Add HemOnc metadata."""
        meta = {
            "data_license": "CC BY 4.0",
            "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",
            "version": self._version,
            "data_url": "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/9CY9C6",  # noqa: E501
            "rdp_url": None,
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": False,
                "attribution": True,
            },
        }
        assert SourceMeta(**meta)
        meta["src_name"] = SourceName.HEMONC.value
        self.database.metadata.put_item(Item=meta)

    def _get_concepts(self) -> Tuple[Dict, Dict, Dict]:
        """Get therapy, brand name, and disease concepts from concepts file.
        :return: Tuple of dicts mapping ID to object for each type of concept
        """
        therapies: RecordParams = {}  # hemonc id -> record
        brand_names: Dict[str, str] = {}  # hemonc id -> brand name
        conditions: Dict[str, str] = {}  # hemonc id -> condition name

        concepts_file = open(self._src_files[0], "r")
        concepts_reader = csv.reader(concepts_file)
        next(concepts_reader)  # skip header
        for row in concepts_reader:
            if row[6]:
                continue  # skip if deprecated/invalid

            row_type = row[2]
            if row_type == "Component":
                concept_id = f"{NamespacePrefix.HEMONC.value}:{row[3]}"
                therapies[row[3]] = {
                    "concept_id": concept_id,
                    "label": row[0],
                    "trade_names": [],
                    "aliases": [],
                    "xrefs": [],
                }
            elif row_type == "Brand Name":
                brand_names[row[3]] = row[0]
            elif row_type == "Condition":
                conditions[row[3]] = row[0]
        concepts_file.close()

        return therapies, brand_names, conditions

    @staticmethod
    def _id_to_yr(hemonc_id: str) -> str:
        """Get year from HemOnc ID corresponding to year concept.
        :param str hemonc_id: HemOnc ID to get year for
        :return: str representing year. Raises TypeError if HemOnc ID not valid.
        """
        id_int = int(hemonc_id)
        if id_int == 780:
            return "9999"
        elif id_int == 48349:
            return "2020"
        elif id_int == 5963:
            return "2021"
        elif id_int < 699 or id_int > 780:
            raise TypeError("ID not a valid HemOnc year concept")
        else:
            return str(id_int + 1240)

    def _get_rels(self, therapies: Dict, brand_names: Dict,
                  conditions: Dict) -> Dict:
        """Gather relations to provide associations between therapies, brand names,
        and conditions.

        :param dict therapies: mapping from IDs to therapy concepts
        :param dict brand_names: mapping from IDs to therapy brand names
        :param dict conditions: mapping from IDs to disease conditions
        :return: therapies dict updated with brand names and conditions
        """
        rels_file = open(self._src_files[1], "r")
        rels_reader = csv.reader(rels_file)
        next(rels_reader)  # skip header

        for row in rels_reader:
            rel_type = row[4]
            hemonc_id = row[0]
            record = therapies.get(hemonc_id)

            if record is None:
                continue  # skip non-drug items

            if rel_type == "Maps to":
                src_raw = row[3]
                if src_raw == "RxNorm":
                    xref = f"{NamespacePrefix.RXNORM.value}:{row[1]}"
                    record["xrefs"].append(xref)
                elif src_raw == "RxNorm Extension":
                    continue  # skip
                else:
                    logger.warning(f"Unrecognized `Maps To` source: {src_raw}")

            elif rel_type == "Has brand name":
                record["trade_names"].append(brand_names[row[1]])

            elif rel_type == "Was FDA approved yr":
                try:
                    year = self._id_to_yr(row[1])
                except TypeError:
                    logger.error(f"Failed parse of FDA approval year ID "
                                 f"{row[1]} for HemOnc ID {row[0]}")
                    continue
                if year == "9999":
                    logger.warning(f"HemOnc ID {row[0]} has FDA approval year"
                                   f" 9999")
                record["approval_ratings"] = [ApprovalRating.HEMONC_APPROVED.value]
                if "approval_year" in record:
                    record["approval_year"].append(year)
                else:
                    record["approval_year"] = [year]

            elif rel_type == "Has FDA indication":
                label = conditions[row[1]]
                norm_id = self._normalize_disease(label)
                hemonc_concept_id = f"{NamespacePrefix.HEMONC.value}:{row[1]}"
                indication = {
                    "disease_id": hemonc_concept_id,
                    "disease_label": label,
                    "normalized_disease_id": norm_id,
                    "supplemental_info": {"regulatory_body": "FDA"}
                }
                if "has_indication" in record:
                    record["has_indication"].append(indication)
                else:
                    record["has_indication"] = [indication]

        rels_file.close()
        return therapies

    def _get_synonyms(self, therapies: Dict) -> Dict:
        """Gather synonym entries and associate with therapy concepts.

        :param dict therapies: mapping of IDs to therapy objects
        :return: therapies dict with synonyms added as aliases
        """
        synonyms_file = open(self._src_files[2], "r")
        synonyms_reader = csv.reader(synonyms_file)
        next(synonyms_reader)
        for row in synonyms_reader:
            therapy_code = row[1]
            if therapy_code in therapies:
                therapy = therapies[therapy_code]
                alias = row[0]
                if alias != therapy.get("label"):
                    therapies[therapy_code]["aliases"].append(row[0])
        synonyms_file.close()
        return therapies

    def _transform_data(self) -> None:
        """Prepare dataset for loading into normalizer database."""
        therapies, brand_names, conditions = self._get_concepts()
        therapies = self._get_rels(therapies, brand_names, conditions)
        therapies = self._get_synonyms(therapies)

        for therapy in therapies.values():
            self._load_therapy(therapy)
