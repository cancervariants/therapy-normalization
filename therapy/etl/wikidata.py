"""This module defines the Wikidata ETL methods."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.schemas import SourceName, NamespacePrefix, \
    SourceIDAfterNamespace, Meta
import json
import logging
from typing import Dict, List
from pathlib import Path
from wikibaseintegrator import wbi_core
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
                 data_path: Path = PROJECT_ROOT / 'data' / 'wikidata'):
        """Initialize wikidata ETL class.

        :param therapy.database.Database: DB instance to use
        :param pathlib.Path data_path: path to wikidata data directory
        """
        self.database = database
        self._data_path = data_path
        self._added_ids = []

    def perform_etl(self) -> List[str]:
        """Public-facing method to initiate ETL procedures on given data.

        :return: List of concept IDs which were successfully processed and
            uploaded.
        """
        self._extract_data()
        self._load_meta()
        self._transform_data()
        return self._added_ids

    def _extract_data(self):
        """Extract data from the Wikidata source."""
        self._data_path.mkdir(exist_ok=True, parents=True)

        data = (wbi_core.FunctionsEngine.execute_sparql_query(
            SPARQL_QUERY))['results']['bindings']

        transformed_data = list()
        for item in data:
            params = dict()
            for attr in item:
                params[attr] = item[attr]['value']
            transformed_data.append(params)

        self._version = datetime.datetime.today().strftime('%Y%m%d')
        with open(f"{self._data_path}/wikidata_{self._version}.json",
                  'w+') as f:
            json.dump(transformed_data, f)
        self._data_src = sorted(list(self._data_path.iterdir()))[-1]
        logger.info('Successfully extracted Wikidata.')

    def _load_meta(self):
        """Add Wikidata metadata."""
        metadata = Meta(src_name=SourceName.WIKIDATA.value,
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
        from therapy import OTHER_IDENTIFIERS
        with open(self._data_src, 'r') as f:
            records = json.load(f)

            items = dict()

            for record in records:
                record_id = record['item'].split('/')[-1]
                concept_id = f"{NamespacePrefix.WIKIDATA.value}:{record_id}"
                if concept_id not in items.keys():
                    item = dict()
                    item['label_and_type'] = f"{concept_id.lower()}##identity"
                    item['concept_id'] = concept_id
                    item['src_name'] = SourceName.WIKIDATA.value

                    other_ids = list()
                    xrefs = list()
                    for key in IDENTIFIER_PREFIXES.keys():
                        if key in record.keys():
                            other_id = record[key]

                            if key.upper() == 'CASREGISTRY':
                                key = SourceName.CHEMIDPLUS.value

                            if key.upper() in OTHER_IDENTIFIERS:
                                if key != 'chembl':
                                    fmted_other_id = \
                                        f"{IDENTIFIER_PREFIXES[key]}:" \
                                        f"{SourceIDAfterNamespace[key.upper()].value}{other_id}"  # noqa: E501
                                else:
                                    fmted_other_id = \
                                        f"{IDENTIFIER_PREFIXES[key]}:" \
                                        f"{other_id}"
                                other_ids.append(fmted_other_id)
                            else:
                                fmted_xref = f"{IDENTIFIER_PREFIXES[key]}:" \
                                             f"{other_id}"
                                xrefs.append(fmted_xref)
                    item['other_identifiers'] = other_ids
                    item['xrefs'] = xrefs
                    if 'itemLabel' in record.keys():
                        item['label'] = record['itemLabel']
                    items[concept_id] = item
                if 'alias' in record.keys():
                    if 'aliases' in items[concept_id].keys():
                        items[concept_id]['aliases'].append(record['alias'])
                    else:
                        items[concept_id]['aliases'] = [record['alias']]

        with self.database.therapies.batch_writer() as batch:
            for item in items.values():
                self._load_therapy(item, batch)

    def _load_therapy(self, item: Dict, batch):
        """Load individual therapy record into database.

        :param Dict item: containing, at minimum, label_and_type and concept_id
            keys.
        :param batch: boto3 batch writer
        """
        if 'aliases' in item:
            item['aliases'] = list(set(item['aliases']))

            if len({a.casefold() for a in item['aliases']}) > 20:  # noqa: E501
                del item['aliases']

        batch.put_item(Item=item)
        self._added_ids.append(item['concept_id'])
        concept_id_lower = item['concept_id'].lower()

        if 'aliases' in item.keys():
            aliases = {alias.lower() for alias in item['aliases']}
            for alias in aliases:
                pk = f"{alias}##alias"
                batch.put_item(Item={
                    'label_and_type': pk,
                    'concept_id': concept_id_lower,
                    'src_name': SourceName.WIKIDATA.value
                })

        if 'label' in item.keys():
            pk = f"{item['label'].lower()}##label"
            batch.put_item(Item={
                'label_and_type': pk,
                'concept_id': concept_id_lower,
                'src_name': SourceName.WIKIDATA.value
            })

        if 'other_identifiers' in item.keys():
            other_ids = {other_id.lower() for other_id
                         in item['other_identifiers']}
            for other_id in other_ids:
                pk = f"{other_id}##other_id"
                batch.put_item(Item={
                    'label_and_type': pk,
                    'concept_id': concept_id_lower,
                    'src_name': SourceName.WIKIDATA.value
                })
