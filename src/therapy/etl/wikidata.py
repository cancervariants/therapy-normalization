"""Defines the Wikidata ETL methods."""
import datetime
import json
import logging
from pathlib import Path
from typing import Any, Dict, Type

from wags_tails import CustomData
from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy import XREF_SOURCES, DownloadException
from therapy.etl.base import Base
from therapy.schemas import NamespacePrefix, RecordParams, SourceMeta, SourceName

logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)

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
  ?drugbank ?alias
WHERE {
  { ?item (wdt:P31/(wdt:P279*)) wd:Q12140. }
  UNION
  { ?item (wdt:P366/(wdt:P279*)) wd:Q12140. }
  UNION
  { ?item (wdt:P31/(wdt:P279*)) wd:Q35456. }
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
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""


class Wikidata(Base):
    """Class for Wikidata ETL methods."""

    @staticmethod
    def _download_data(version: str, file: Path) -> None:
        """Download latest Wikidata source dump.

        :param version: not used by this method
        :param file: location to save data dump at
        :raise DownloadException: if SPARQL query fails
        """
        medicine_query_results = execute_sparql_query(SPARQL_QUERY)
        if medicine_query_results is None:
            raise DownloadException("Wikidata medicine SPARQL query failed")
        results = medicine_query_results["results"]["bindings"]

        transformed_data = []
        for item in results:
            params: RecordParams = {}
            for attr in item:
                params[attr] = item[attr]["value"]
            transformed_data.append(params)
        with open(file, "w+") as f:
            json.dump(transformed_data, f)

    @staticmethod
    def _get_latest_version() -> str:
        """Wikidata is constantly, immediately updated, so source data has no strict
        versioning. We use the current date as a pragmatic way to indicate the version.
        """
        return datetime.datetime.today().strftime("%Y-%m-%d")

    _DataSourceClass: Type[CustomData] = CustomData

    def _extract_data(self, use_existing: bool) -> None:
        """Acquire source data.

        This method is responsible for initializing an instance of
        ``self._DataSourceClass``, and, in most cases, setting ``self._src_file``.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        data_source: CustomData = self._DataSourceClass(
            src_name="wikidata_drugs",
            filetype="json",
            latest_version_cb=self._get_latest_version,
            download_cb=self._download_data,
            data_dir=self._therapy_data_dir,
        )  # type: ignore
        self._src_file, self._version = data_source.get_latest(from_local=use_existing)

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
        params = dict(metadata)
        params["src_name"] = SourceName.WIKIDATA.value
        self.database.metadata.put_item(Item=params)

    def _transform_data(self) -> None:
        """Transform the Wikidata source data."""
        with open(self._src_file, "r") as f:
            records = json.load(f)

            items: Dict[str, Any] = dict()

            for record in records:
                record_id = record["item"].split("/")[-1]
                concept_id = f"{NamespacePrefix.WIKIDATA.value}:{record_id}"
                if concept_id not in items:
                    item: Dict[str, Any] = {"concept_id": concept_id}

                    xrefs = list()
                    associated_with = list()
                    for key in NAMESPACES.keys():
                        if key in record.keys():
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
                                fmted_assoc = f"{NAMESPACES[key]}:" f"{ref}"
                                associated_with.append(fmted_assoc)
                    item["xrefs"] = xrefs
                    item["associated_with"] = associated_with
                    if "itemLabel" in record.keys():
                        item["label"] = record["itemLabel"]
                    items[concept_id] = item
                if "alias" in record.keys():
                    if "aliases" in items[concept_id].keys():
                        items[concept_id]["aliases"].append(record["alias"])
                    else:
                        items[concept_id]["aliases"] = [record["alias"]]

        for item in items.values():
            self._load_therapy(item)
