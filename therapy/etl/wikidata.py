"""This module defines the Wikidata ETL methods."""
import json
import logging
import datetime
from typing import Dict, Any

from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy import XREF_SOURCES, DownloadException
from therapy.schemas import SourceName, NamespacePrefix, RecordParams, SourceMeta
from therapy.etl.base import Base

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
    SELECT ?item ?itemLabel ?casRegistry ?pubchemCompound
           ?pubchemSubstance ?chembl
           ?rxnorm ?drugbank ?alias WHERE {
      ?item (wdt:P31/(wdt:P279*)) wd:Q12140.
      OPTIONAL {
        ?item skos:altLabel ?alias.
        FILTER((LANG(?alias)) = \"en\")
      }
      OPTIONAL { ?item p:P231 ?wds1.
                 ?wds1 ps:P231 ?casRegistry.
               }
      OPTIONAL { ?item p:P662 ?wds2.
                 ?wds2 ps:P662 ?pubchemCompound.
               }
      OPTIONAL { ?item p:P2153 ?wds3.
                 ?wds3 ps:P2153 ?pubchemSubstance.
               }
      OPTIONAL { ?item p:P592 ?wds4.
                 ?wds4 ps:P592 ?chembl
               }
      OPTIONAL { ?item p:P3345 ?wds5.
                 ?wds5 ps:P3345 ?rxnorm.
               }
      OPTIONAL { ?item p:P715 ?wds6.
                 ?wds6 ps:P715 ?drugbank
               }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\".
      }
    }
"""


class Wikidata(Base):
    """Class for Wikidata ETL methods."""

    def _download_data(self) -> None:
        """Download latest Wikidata source dump."""
        logger.info("Retrieving source data for Wikidata")
        query_results = execute_sparql_query(SPARQL_QUERY)
        if query_results is None:
            raise DownloadException("Wikidata SPARQL query returned no results")
        else:
            data = query_results["results"]["bindings"]

        transformed_data = []
        for item in data:
            params: RecordParams = {}
            for attr in item:
                params[attr] = item[attr]["value"]
            transformed_data.append(params)
        with open(f"{self._src_dir}/wikidata_{self._version}.json", "w+") as f:
            json.dump(transformed_data, f)
        logger.info("Successfully retrieved source data for Wikidata")

    def get_latest_version(self) -> str:
        """Wikidata is constantly, immediately updated, so source data has no strict
        versioning. We use the current date as a pragmatic way to indicate the version.
        """
        return datetime.datetime.today().strftime("%Y-%m-%d")

    def _load_meta(self) -> None:
        """Add Wikidata metadata."""
        metadata = SourceMeta(
            src_name=SourceName.WIKIDATA.value,
            data_license="CC0 1.0",
            data_license_url="https://creativecommons.org/publicdomain/zero/1.0/",
            version=self._version,
            data_url=None,
            rdp_url=None,
            data_license_attributes={
                "non_commercial": False,
                "share_alike": False,
                "attribution": False
            }
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
                if concept_id not in items.keys():
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
                                fmted_assoc = f"{NAMESPACES[key]}:" \
                                              f"{ref}"
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
