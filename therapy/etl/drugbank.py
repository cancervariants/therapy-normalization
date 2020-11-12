"""This module defines the DrugBank ETL methods."""
from therapy.etl.base import Base
from therapy import PROJECT_ROOT
import logging
from therapy import database, schemas  # noqa: F401
from therapy.schemas import SourceName, NamespacePrefix, ApprovalStatus
from therapy.etl.base import IDENTIFIER_PREFIXES
from lxml import etree
import json

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class DrugBank(Base):
    """ETL the DrugBank source into therapy.db."""

    def _extract_data(self, *args, **kwargs):
        """Extract data from the DrugBank source."""
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            wd_dir = PROJECT_ROOT / 'data' / 'drugbank'
            try:
                self._data_src = sorted(list(wd_dir.iterdir()))[-1]
            except IndexError:
                raise FileNotFoundError  # TODO drugbank update function here

    def _transform_data(self):
        """Transform the DrugBank source."""
        xmlns = "{http://www.drugbank.ca}"

        tree = etree.parse(f"{self._data_src}")
        root = tree.getroot()
        records_list = []
        for drug in root:
            params = {
                'label_and_type': None,
                'concept_id': None,
                'label': None,
                'approval_status': None,
                'aliases': [],
                'other_identifiers': [],
                'trade_names': [],
                'src_name': SourceName.DRUGBANK.value
            }
            for element in drug:
                if element.tag == f"{xmlns}drugbank-id":
                    # Concept ID
                    if 'primary' in element.attrib:
                        params['concept_id'] = \
                            f"{NamespacePrefix.DRUGBANK.value}:{element.text}"
                        params['label_and_type'] = \
                            f"{params['concept_id'].lower()}##identity"
                    else:
                        # Aliases
                        params['aliases'].append(element.text)
                # Label
                if element.tag == f"{xmlns}name":
                    params['label'] = element.text
                # Aliases
                if element.tag == f"{xmlns}synonyms":
                    for alias in element:
                        if alias.text not in params['aliases'] and \
                                alias.attrib['language'] == 'english':
                            params['aliases'].append(alias.text)
                # Trade Names
                if element.tag == f"{xmlns}products":
                    for product in element:
                        for el in product:
                            if el.tag == f"{xmlns}name":
                                if el.text not in params['trade_names']:
                                    params['trade_names'].append(el.text)
                # Other Identifiers
                if element.tag == f"{xmlns}cas-number":
                    if element.text:
                        params['other_identifiers'].append(
                            f"{IDENTIFIER_PREFIXES['casRegistry']}:"
                            f"{element.text}")
                # Approval status
                if element.tag == f"{xmlns}groups":
                    group_type = []
                    for group in element:
                        group_type.append(group.text)
                    if "withdrawn" in group_type:
                        params['approval_status'] = \
                            ApprovalStatus.WITHDRAWN.value
                    elif "approved" in group_type:
                        params['approval_status'] = \
                            ApprovalStatus.APPROVED.value
                    elif "investigational" in group_type:
                        params['approval_status'] = \
                            ApprovalStatus.INVESTIGATIONAL.value

            records_list.append(params)
            if params['label']:
                self._load_label(params['label'], params['concept_id'],
                                 records_list)
            if params['aliases']:
                self._load_aliases(params['aliases'], params['concept_id'],
                                   records_list)
            if params['trade_names']:
                self._load_trade_names(params['trade_names'],
                                       params['concept_id'], records_list)
        return records_list

    def _load_data(self, *args, **kwargs):
        """Load the DrugBank source into normalized database."""
        self._extract_data()
        records_list = self._transform_data()
        self._load_json(records_list)
        self._add_meta()

    def _load_json(self, records_list):
        """Load DrugBank data into JSON file."""
        with open('data/drugbank/drugbank.json', 'w') as f:
            f.write(json.dumps(records_list))

    def _load_label(self, label, concept_id, records_list):
        """Insert label data into records_list."""
        label = {
            'label_and_type':
                f"{label.lower()}##label",
            'concept_id': f"{concept_id.lower()}"
        }
        records_list.append(label)

    def _load_aliases(self, aliases, concept_id, records_list):
        """Insert alias data into records_list."""
        aliases = list(set({a.casefold(): a for a in aliases}.values()))
        for alias in aliases:
            alias = {
                'label_and_type': f"{alias.lower()}##alias",
                'concept_id': f"{concept_id.lower()}"
            }
            records_list.append(alias)

    def _load_trade_names(self, trade_names, concept_id, records_list):
        """Insert trade_name data into records_list."""
        trade_names = \
            list(set({t.casefold(): t for t in trade_names}.values()))
        for trade_name in trade_names:
            trade_name = {
                'label_and_type': f"{trade_name.lower()}##trade_name",
                'concept_id': f"{concept_id.lower()}"
            }
            records_list.append(trade_name)

    def _add_meta(self):
        """Add DrugBank metadata."""
        # TODO: Add to MetaData table
        src_name = SourceName.DRUGBANK.value  # noqa: F841
        data_license = 'CC BY-NC 4.0'  # noqa: F841
        data_license_url = 'https://creativecommons.org/licenses/by-nc/4.0/legalcode'  # noqa: E501, F841
        version = '5.1.7'  # noqa: F841
        data_url = 'https://go.drugbank.com/releases/5-1-7/downloads/all-full-database'  # noqa E501, F481
