"""This module defines the Wikidata ETL methods."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.schemas import SourceName, NamespacePrefix, \
    SourceIDAfterNamespace, SourceMeta
import json
import logging
from pathlib import Path
from wikibaseintegrator.wbi_functions import execute_sparql_query
import datetime

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)

# Prefixes for translating ID namespaces
IDENTIFIER_PREFIXES = {
    'casRegistry': NamespacePrefix.CASREGISTRY.value,
    'ChemIDplus': NamespacePrefix.CHEMIDPLUS.value,
    'pubchemCompound': NamespacePrefix.PUBCHEMCOMPOUND.value,
    'pubchemSubstance': NamespacePrefix.PUBCHEMSUBSTANCE.value,
    'chembl': NamespacePrefix.CHEMBL.value,
    'rxnorm': NamespacePrefix.RXNORM.value,
    'drugbank': NamespacePrefix.DRUGBANK.value,
    'wikidata': NamespacePrefix.WIKIDATA.value,
}


SPARQL_QUERY = """
    SELECT ?item ?itemLabel ?casRegistry ?pubchemCompound
           ?pubchemSubstance ?chembl
           ?rxnorm ?drugbank ?alias WHERE {
      ?item (wdt:P31/(wdt:P279*)) wd:Q12140.
      OPTIONAL {
        ?item skos:altLabel ?alias.
        FILTER((LANG(?alias)) = "en")
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
        bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
      }
    }
"""


class Wikidata(Base):
    """Extract, transform, and load the Wikidata source into therapy.db."""

    def __init__(self,
                 database,
                 data_path: Path = PROJECT_ROOT / 'data'):
        """Initialize wikidata ETL class.

        :param therapy.database.Database: DB instance to use
        :param Path data_path: path to app data directory
        """
        super().__init__(database, data_path)

    def _extract_data(self):
        """Extract data from the Wikidata source."""
        self._src_data_dir.mkdir(exist_ok=True, parents=True)

        data = execute_sparql_query(SPARQL_QUERY)['results']['bindings']

        transformed_data = list()
        for item in data:
            params = dict()
            for attr in item:
                params[attr] = item[attr]['value']
            transformed_data.append(params)

        self._version = datetime.datetime.today().strftime('%Y%m%d')
        with open(f"{self._src_data_dir}/wikidata_{self._version}.json",
                  'w+') as f:
            json.dump(transformed_data, f)
        self._data_src = sorted(list(self._src_data_dir.iterdir()))[-1]
        logger.info('Successfully extracted Wikidata.')

    def _load_meta(self):
        """Add Wikidata metadata."""
        metadata = SourceMeta(src_name=SourceName.WIKIDATA.value,
                              data_license='CC0 1.0',
                              data_license_url='https://creativecommons.org/publicdomain/zero/1.0/',  # noqa: E501
                              version=self._version,
                              data_url=None,
                              rdp_url=None,
                              data_license_attributes={
                                  'non_commercial': False,
                                  'share_alike': False,
                                  'attribution': False
                              })
        params = dict(metadata)
        params['src_name'] = SourceName.WIKIDATA.value
        self.database.metadata.put_item(Item=params)

    def _transform_data(self):
        """Transform the Wikidata source data."""
        from therapy import XREF_SOURCES
        with open(self._data_src, 'r') as f:
            records = json.load(f)

            items = dict()

            for record in records:
                record_id = record['item'].split('/')[-1]
                concept_id = f"{NamespacePrefix.WIKIDATA.value}:{record_id}"
                if concept_id not in items.keys():
                    item = dict()
                    item['label_and_type'] = f"{concept_id.lower()}##identity"
                    item['item_type'] = 'identity'
                    item['concept_id'] = concept_id
                    item['src_name'] = SourceName.WIKIDATA.value

                    xrefs = list()
                    associated_with = list()
                    for key in IDENTIFIER_PREFIXES.keys():
                        if key in record.keys():
                            ref = record[key]

                            if key.upper() == 'CASREGISTRY':
                                key = SourceName.CHEMIDPLUS.value

                            if key.upper() in XREF_SOURCES:
                                if key != 'chembl':
                                    fmted_xref = \
                                        f"{IDENTIFIER_PREFIXES[key]}:{SourceIDAfterNamespace[key.upper()].value}{ref}"  # noqa: E501
                                else:
                                    fmted_xref = \
                                        f"{IDENTIFIER_PREFIXES[key]}:{ref}"
                                xrefs.append(fmted_xref)
                            else:
                                fmted_assoc = f"{IDENTIFIER_PREFIXES[key]}:" \
                                              f"{ref}"
                                associated_with.append(fmted_assoc)
                    item['xrefs'] = xrefs
                    item['associated_with'] = associated_with
                    if 'itemLabel' in record.keys():
                        item['label'] = record['itemLabel']
                    items[concept_id] = item
                if 'alias' in record.keys():
                    if 'aliases' in items[concept_id].keys():
                        items[concept_id]['aliases'].append(record['alias'])
                    else:
                        items[concept_id]['aliases'] = [record['alias']]

        for item in items.values():
            self._load_therapy(item)
