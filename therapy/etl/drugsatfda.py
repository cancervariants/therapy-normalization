"""ETL methods for the Drugs@FDA source."""
from typing import List, Union, Dict
import zipfile
from io import BytesIO
import json
import shutil

import requests

from .base import Base
from therapy import DownloadException, logger
from therapy.schemas import SourceMeta, SourceName, NamespacePrefix, \
    ApprovalStatus


class DrugsAtFDA(Base):
    """Core Drugs@FDA import class."""

    def _download_data(self) -> None:
        """Download source data from instance-provided source URL."""
        logger.info("Retrieving source data for Drugs@FDA")
        r = requests.get("https://download.open.fda.gov/drug/drugsfda/drug-drugsfda-0001-of-0001.json.zip")  # noqa: E501
        if r.status_code == 200:
            zip_file = zipfile.ZipFile(BytesIO(r.content))
        else:
            msg = f"Drugs@FDA download failed with status code: {r.status_code}"  # noqa: E501
            logger.error(msg)
            raise requests.HTTPError(r.status_code)
        orig_fname = "drug-drugsfda-0001-of-0001.json"
        zip_file.extract(member=orig_fname, path=self._src_data_dir)
        version = self._get_latest_version()
        outfile_path = self._src_data_dir / f"drugsatfda_{version}.json"
        shutil.move(self._src_data_dir / orig_fname, outfile_path)
        logger.info("Successfully retrieved source data for Drugs@FDA")

    def _get_latest_version(self) -> str:
        """Retrieve latest version of source data.
        :return: version as a str -- expected formatting is YYYY-MM-DD
        """
        r = requests.get("https://api.fda.gov/download.json")
        if r.status_code == 200:
            json = r.json()
            try:
                return json["results"]["drug"]["drugsfda"]["export_date"]
            except KeyError:
                msg = "Unable to parse OpenFDA version API - check for breaking changes"  # noqa: E501
                logger.error(msg)
                raise DownloadException(msg)
        else:
            raise requests.HTTPError("Unable to retrieve version from FDA API")

    def _load_meta(self):
        """Add Drugs@FDA metadata."""
        meta = {
            "data_license": "CC0",
            "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",  # noqa: E501
            "version": self._version,
            "data_url": "https://open.fda.gov/apis/drug/drugsfda/download/",
            "rdp_url": None,
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": False,
                "attribution": False,
            }
        }
        assert SourceMeta(**meta)
        meta["src_name"] = SourceName.DRUGSATFDA
        self.database.metadata.put_item(Item=meta)

    def _transform_data(self):
        """Prepare source data for loading into DB."""
        with open(self._src_file, "r") as f:
            data = json.load(f)["results"]

        statuses_map = {
            "Discontinued": ApprovalStatus.FDA_DISCONTINUED.value,
            "Prescription": ApprovalStatus.FDA_PRESCRIPTION.value,
            "Over-the-counter": ApprovalStatus.FDA_OTC.value,
            "None (Tentative Approval)": ApprovalStatus.FDA_TENTATIVE.value,
        }

        for result in data:
            concept_id = f"{NamespacePrefix.DRUGSATFDA.value}:{result['application_number']}"  # noqa: E501
            therapy: Dict[str, Union[str, List]] = {"concept_id": concept_id}
            if "products" not in result:
                continue
            products = result["products"]

            statuses = [p["marketing_status"] for p in products]
            if not all([s == statuses[0] for s in statuses]):
                msg = (f"Application {concept_id} has inconsistent marketing "
                       f"statuses: {statuses}")
                logger.info(msg)
                continue
            status = statuses_map.get(statuses[0])
            if status:
                therapy["approval_status"] = status

            brand_names = [p["brand_name"] for p in products]

            aliases = []

            if "openfda" in result:
                openfda = result["openfda"]
                brand_name = openfda.get("brand_name")
                if brand_name:
                    brand_names += brand_name

                substance = openfda.get("substance_name", [])
                num_substances = len(substance)
                if num_substances > 1:
                    msg = (f"Application {concept_id} has >1 substance names: "
                           f"{substance}")
                    logger.info(msg)
                elif num_substances == 1:
                    therapy["label"] = substance[0]

                generic = openfda.get("generic_name", [])
                if len(generic) > 1:
                    msg = (f"Application {concept_id} has >1 generic names: "
                           f"{generic}")
                    logger.info(msg)
                elif len(generic) == 1:
                    if num_substances == 0:
                        therapy["label"] = generic[0]
                    else:
                        aliases.append(generic[0])

                unii = openfda.get("unii")
                if unii:
                    therapy["associated_with"] = [f"{NamespacePrefix.UNII.value}:{u}" for u in unii]  # noqa: E501
                rxcui = openfda.get("rxcui")
                if rxcui:
                    therapy["xrefs"] = [f"{NamespacePrefix.RXNORM.value}:{r}" for r in rxcui]  # noqa: E501

            therapy["trade_names"] = brand_names
            therapy["aliases"] = aliases
            self._load_therapy(therapy)
