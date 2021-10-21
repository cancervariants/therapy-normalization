"""ETL methods for the Drugs@FDA source."""
from typing import List, Union, Dict, Optional
import zipfile
from io import BytesIO
import json
import shutil

import requests

from therapy import DownloadException, logger
from therapy.schemas import SourceMeta, SourceName, NamespacePrefix, \
    ApprovalStatus
from therapy.etl.base import Base


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
        shutil.move(str(self._src_data_dir / orig_fname), str(outfile_path))
        logger.info("Successfully retrieved source data for Drugs@FDA")

    def _get_latest_version(self) -> str:
        """Retrieve latest version of source data.
        :return: version as a str -- expected formatting is YYYY-MM-DD
        """
        r = requests.get("https://api.fda.gov/download.json")
        if r.status_code == 200:
            json = r.json()
            try:
                date_raw = json["results"]["drug"]["drugsfda"]["export_date"]
            except KeyError:
                msg = "Unable to parse OpenFDA version API - check for breaking changes"  # noqa: E501
                logger.error(msg)
                raise DownloadException(msg)
            return date_raw.replace("-", "")
        else:
            raise requests.HTTPError("Unable to retrieve version from FDA API")

    def _load_meta(self) -> None:
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

    def _get_marketing_status(self, products: List, concept_id: str)\
            -> Optional[str]:
        """Get approval status from products list.
        :param List products: list of individual FDA product objects
        :param str concept_id: FDA application concept ID, used in reporting
            error messages
        :return: approval_status value if successful, None if ambiguous or
            unavailable
        """
        statuses_map = {
            "Discontinued": ApprovalStatus.FDA_DISCONTINUED.value,
            "Prescription": ApprovalStatus.FDA_PRESCRIPTION.value,
            "Over-the-counter": ApprovalStatus.FDA_OTC.value,
            "None (Tentative Approval)": ApprovalStatus.FDA_TENTATIVE.value,
        }
        statuses = [p["marketing_status"] for p in products]
        if not all([s == statuses[0] for s in statuses]):
            msg = (f"Application {concept_id} has inconsistent marketing "
                   f"statuses: {statuses}")
            logger.info(msg)
            return None
        else:
            return statuses_map.get(statuses[0])

    def _transform_data(self) -> None:
        """Prepare source data for loading into DB."""
        with open(self._src_file, "r") as f:
            data = json.load(f)["results"]

        for result in data:
            if "products" not in result:
                continue
            products = result["products"]

            app_no = result["application_number"]
            if app_no.startswith("BLA"):
                # ignore biologics license applications (tentative)
                continue
            concept_id = f"{NamespacePrefix.DRUGSATFDA.value}:{app_no}"
            therapy: Dict[str, Union[str, List]] = {"concept_id": concept_id}

            status = self._get_marketing_status(products, concept_id)
            if status:
                therapy["approval_status"] = status

            brand_names = [p["brand_name"] for p in products]

            aliases = []

            if "openfda" in result:
                openfda = result["openfda"]
                brand_name = openfda.get("brand_name")
                if brand_name:
                    brand_names += brand_name

                substances = openfda.get("substance_name", [])
                n_substances = len(set(substances))
                if n_substances > 1:
                    # if ambiguous, store all as aliases
                    msg = (f"Application {concept_id} has {n_substances} "
                           f"substance names: {substances}")
                    logger.debug(msg)
                    aliases += substances
                elif n_substances == 1:
                    therapy["label"] = substances[0]

                generics = openfda.get("generic_name", [])
                n_generic = len(set(generics))
                if n_generic > 1:
                    aliases += generics
                elif n_generic == 1:
                    if n_substances == 0:
                        # there are about 300 cases of this
                        therapy["label"] = generics[0]
                    else:
                        aliases.append(generics[0])

                unii = openfda.get("unii")
                if unii:
                    therapy["associated_with"] = [f"{NamespacePrefix.UNII.value}:{u}" for u in unii]  # noqa: E501
                rxcui = openfda.get("rxcui")
                if rxcui:
                    therapy["xrefs"] = [f"{NamespacePrefix.RXNORM.value}:{r}" for r in rxcui]  # noqa: E501

            therapy["trade_names"] = brand_names
            therapy["aliases"] = aliases
            self._load_therapy(therapy)
