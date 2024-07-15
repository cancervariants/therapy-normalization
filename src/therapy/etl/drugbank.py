"""Defines the DrugBank ETL methods."""

import csv
from typing import Any

from tqdm import tqdm

from therapy.etl.base import Base
from therapy.schemas import NamespacePrefix, SourceMeta, SourceName


class DrugBank(Base):
    """Class for DrugBank ETL methods."""

    def _load_meta(self) -> None:
        """Add DrugBank metadata."""
        metadata = SourceMeta(
            data_license="CC0 1.0",
            data_license_url="https://creativecommons.org/publicdomain/zero/1.0/",
            version=self._version,
            data_url="https://go.drugbank.com/releases/latest#open-data",
            rdp_url="http://reusabledata.org/drugbank.html",
            data_license_attributes={
                "non_commercial": False,
                "share_alike": False,
                "attribution": False,
            },
        )
        self.database.add_source_metadata(SourceName.DRUGBANK, metadata)

    def _transform_data(self) -> None:
        """Transform the DrugBank source."""
        with self._data_file.open() as file:  # type: ignore
            reader = list(csv.reader(file))
            for row in tqdm(reader[1:], ncols=80, disable=self._silent):
                # get concept ID
                params: dict[str, Any] = {
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
