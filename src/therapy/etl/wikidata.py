"""Defines the Wikidata ETL methods."""

import datetime
import json
import os
from pathlib import Path
from typing import Any

from tqdm import tqdm
from wags_tails import CustomData, DataSource
from wags_tails.utils.versioning import DATE_VERSION_PATTERN
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy import XREF_SOURCES
from therapy.etl.base import Base, EtlError
from therapy.schemas import NamespacePrefix, RecordParams, SourceMeta, SourceName

wbi_config["USER_AGENT"] = (
    os.environ.get("WIKIBASE_USER_AGENT")
    or "thera_py (https://github.com/cancervariants/therapy-normalization)"
)

# Translate Wikidata keys to standardized namespaces
NAMESPACES = {
    "casRegistry": NamespacePrefix.CASREGISTRY.value,
    "ChemIDplus": NamespacePrefix.CHEMIDPLUS.value,
    "pubchemCompound": NamespacePrefix.PUBCHEMCOMPOUND.value,
    "pubchemSubstance": NamespacePrefix.PUBCHEMSUBSTANCE.value,
    "chembl": NamespacePrefix.CHEMBL.value,
    "rxnorm": NamespacePrefix.RXNORM.value,
    "drugbank": NamespacePrefix.DRUGBANK.value,
    "wikidata": NamespacePrefix.WIKIDATA.value,
    "guideToPharmacology": NamespacePrefix.GUIDETOPHARMACOLOGY.value,
}

# Provide standard concept ID prefixes
ID_PREFIXES = {
    "wikidata": "Q",
    "chembl": "CHEMBL",
    "drugbank": "DB",
    "ncit": "C",
}

SPARQL_QUERY = """
SELECT
  ?item ?itemLabel ?casRegistry ?pubchemCompound ?pubchemSubstance ?chembl ?rxnorm
  ?drugbank ?guideToPharmacology
  (GROUP_CONCAT(DISTINCT ?alias; separator=";;") AS ?aliases)
WHERE {
  { ?item (wdt:P31/(wdt:P279*)) wd:Q12140. }
  UNION
  { ?item (wdt:P366/(wdt:P279*)) wd:Q12140. }
  UNION
  { ?item (wdt:P31/(wdt:P279*)) wd:Q35456. }
  UNION
  { ?item (wdt:P2868/(wdt:P279*)) wd:Q12187. }
  OPTIONAL {
    ?item skos:altLabel ?alias.
    FILTER((LANG(?alias)) = "en")
  }
  OPTIONAL {
    ?item p:P231 ?wds1.
    ?wds1 ps:P231 ?casRegistry.
  }
  OPTIONAL {
    ?item p:P662 ?wds2.
    ?wds2 ps:P662 ?pubchemCompound.
  }
  OPTIONAL {
    ?item p:P2153 ?wds3.
    ?wds3 ps:P2153 ?pubchemSubstance.
  }
  OPTIONAL {
    ?item p:P592 ?wds4.
    ?wds4 ps:P592 ?chembl.
  }
  OPTIONAL {
    ?item p:P3345 ?wds5.
    ?wds5 ps:P3345 ?rxnorm.
  }
  OPTIONAL {
    ?item p:P715 ?wds6.
    ?wds6 ps:P715 ?drugbank.
  }
  OPTIONAL {
      ?item p:P595 ?wds7.
      ?wds7 ps:P595 ?guideToPharmacology
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
GROUP BY ?item ?itemLabel ?casRegistry ?pubchemCompound ?pubchemSubstance ?chembl
  ?rxnorm ?drugbank ?guideToPharmacology
"""


class Wikidata(Base):
    """Class for Wikidata ETL methods."""

    @staticmethod
    def _download_data(version: str, file: Path) -> None:  # noqa: ARG004
        """Download latest Wikidata source dump.

        :param version: not used by this method
        :param file: location to save data dump at
        :raise EtlError: if SPARQL query fails
        """
        medicine_query_results = execute_sparql_query(SPARQL_QUERY)
        if medicine_query_results is None:
            msg = "Wikidata medicine SPARQL query failed"
            raise EtlError(msg)
        results = medicine_query_results["results"]["bindings"]

        transformed_data = []
        for item in results:
            params: RecordParams = {}
            for attr in item:
                params[attr] = item[attr]["value"]
            transformed_data.append(params)
        with file.open("w+") as f:
            json.dump(transformed_data, f)

    @staticmethod
    def _get_latest_version() -> str:
        """Wikidata is constantly, immediately updated, so source data has no strict
        versioning. We use the current date as a pragmatic way to indicate the version.

        :return: formatted string for the current date
        """
        return datetime.datetime.now(tz=datetime.UTC).strftime(DATE_VERSION_PATTERN)

    def _get_data_handler(self, data_path: Path | None = None) -> DataSource:
        """Construct data handler instance for source. Overwrites base class method
        to use custom data handler instead.

        :param data_path: location of data storage
        :return: instance of wags_tails.DataSource to manage source file(s)
        """
        return CustomData(
            "wikidata",
            "json",
            self._get_latest_version,
            self._download_data,
            data_dir=data_path,
            silent=self._silent,
        )

    def _load_meta(self) -> None:
        """Add Wikidata metadata."""
        metadata = SourceMeta(
            data_license="CC0 1.0",
            data_license_url="https://creativecommons.org/publicdomain/zero/1.0/",
            version=self._version,
            data_url=None,
            rdp_url=None,
            data_license_attributes={
                "non_commercial": False,
                "share_alike": False,
                "attribution": False,
            },
        )
        self.database.add_source_metadata(SourceName.WIKIDATA, metadata)

    def _transform_data(self) -> None:
        """Transform the Wikidata source data."""
        with self._data_file.open() as f:
            records: dict = json.load(f)

            items: dict[str, Any] = {}

            for record in records:
                record_id = record["item"].split("/")[-1]
                concept_id = f"{NamespacePrefix.WIKIDATA.value}:{record_id}"
                if concept_id not in items:
                    item: dict[str, Any] = {"concept_id": concept_id}

                    xrefs = []
                    associated_with = []
                    for key in NAMESPACES:
                        if key in record:
                            ref = record[key]

                            if key.upper() == "CASREGISTRY":
                                key = SourceName.CHEMIDPLUS.value

                            if key.upper() in XREF_SOURCES:
                                if key != "chembl":
                                    prefix = ID_PREFIXES.get(key.lower(), "")
                                    fmted_xref = f"{NAMESPACES[key]}:{prefix}{ref}"
                                else:
                                    fmted_xref = f"{NAMESPACES[key]}:{ref}"
                                xrefs.append(fmted_xref)
                            else:
                                fmted_assoc = f"{NAMESPACES[key]}:{ref}"
                                associated_with.append(fmted_assoc)
                    item["xrefs"] = xrefs
                    item["associated_with"] = associated_with
                    if "itemLabel" in record:
                        item["label"] = record["itemLabel"]
                    items[concept_id] = item
                if "aliases" in record:
                    if "aliases" in items[concept_id]:
                        items[concept_id]["aliases"] += record["aliases"].split(";;")
                    else:
                        items[concept_id]["aliases"] = record["aliases"].split(";;")
        for item in tqdm(items.values(), ncols=80, disable=self._silent):
            self._load_therapy(item)
