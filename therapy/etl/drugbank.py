"""This module defines the DrugBank ETL methods."""
from typing import Dict, Any
import logging
import csv

from therapy.schemas import SourceName, SourceMeta, NamespacePrefix
from therapy.etl.base import Base

logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class DrugBank(Base):
    """Class for DrugBank ETL methods."""

    def _download_data(self) -> None:
        """Download DrugBank source data."""
        logger.info("Retrieving source data for DrugBank")
        url = f"https://go.drugbank.com/releases/{self._version.replace('.', '-')}/downloads/all-drugbank-vocabulary"  # noqa: E501
        csv_file = self._src_dir / f"drugbank_{self._version}.csv"
        self._http_download(url, csv_file, handler=self._zip_handler)
        logger.info("Successfully retrieved source data for DrugBank")

    def _load_meta(self) -> None:
        """Add DrugBank metadata."""
        meta = {
            "data_license": "CC0 1.0",
            "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
            "version": self._version,
            "data_url": "https://go.drugbank.com/releases/latest#open-data",
            "rdp_url": "http://reusabledata.org/drugbank.html",
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": False,
                "attribution": False,
            },
        }
        assert SourceMeta(**meta)
        meta["src_name"] = SourceName.DRUGBANK.value
        self.database.metadata.put_item(Item=meta)

    def _transform_data(self) -> None:
        """Transform the DrugBank source."""
        with open(self._src_file, "r") as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for row in reader:
                # get concept ID
                params: Dict[str, Any] = {
                    "concept_id": f"{NamespacePrefix.DRUGBANK.value}:{row[0]}",
                }

                # get label
                label = row[2]
                if label:
                    params["label"] = label

                # get aliases
                aliases = [a for a in row[1].split(" | ") + row[5].split(" | ") if a]
                if aliases:
                    params["aliases"] = aliases

                # get CAS reference
                cas_ref = row[3]
                if cas_ref:
                    params["xrefs"] = [f"{NamespacePrefix.CHEMIDPLUS.value}:{row[3]}"]

                params["associated_with"] = []
                # get inchi key
                if len(row) >= 7:
                    inchi_key = row[6]
                    if inchi_key:
                        inchi_id = f"{NamespacePrefix.INCHIKEY.value}:{inchi_key}"
                        params["associated_with"].append(inchi_id)
                # get UNII id
                unii = row[4]
                if unii:
                    unii_id = f"{NamespacePrefix.UNII.value}:{unii}"
                    params["associated_with"].append(unii_id)

                self._load_therapy(params)
