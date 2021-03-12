"""This module defines the DrugBank ETL methods."""
from therapy import PROJECT_ROOT
from therapy.schemas import SourceName, NamespacePrefix, ApprovalStatus, Meta
from therapy.etl.base import Base
import logging
from lxml import etree
from typing import List
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth
from os import environ
import zipfile
import shutil
from io import BytesIO

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)

DRUGBANK_IDENTIFIER_PREFIXES = {
    'ChEBI': NamespacePrefix.CHEBI.value,
    'ChEMBL': NamespacePrefix.CHEMBL.value,
    'PubChem Compound': NamespacePrefix.PUBCHEMCOMPOUND.value,
    'PubChem Substance': NamespacePrefix.PUBCHEMSUBSTANCE.value,
    'KEGG Compound': NamespacePrefix.KEGGCOMPOUND.value,
    'KEGG Drug': NamespacePrefix.KEGGDRUG.value,
    'ChemSpider': NamespacePrefix.CHEMSPIDER.value,
    'BindingDB': NamespacePrefix.BINDINGDB.value,
    'PharmGKB': NamespacePrefix.PHARMGKB.value,
    'ZINC': NamespacePrefix.ZINC.value,
    'RxCUI': NamespacePrefix.RXNORM.value,
    'PDB': NamespacePrefix.PDB.value,
    'Therapeutic Targets Database': NamespacePrefix.THERAPEUTICTARGETSDB.value,
    'IUPHAR': NamespacePrefix.IUPHAR.value,
    'Guide to Pharmacology': NamespacePrefix.GUIDETOPHARMACOLOGY.value
}


class DrugBank(Base):
    """ETL the DrugBank source into therapy.db."""

    def __init__(self,
                 database,
                 data_path: Path = PROJECT_ROOT / 'data' / 'drugbank',
                 data_url: str = 'https://go.drugbank.com/releases/5-1-7/'
                                 'downloads/all-full-database',
                 version: str = '5.1.7'):
        """Initialize ETL class instance.

        :param Path data_path: directory containing source data
        """
        self._database = database
        self._data_path = data_path
        self._data_url = data_url
        self._version = version
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

    def _download_data(self):
        """Download DrugBank database XML file.

        :param PosixPath db_dir: The path to the DrugBank data directory
        """
        logger.info("Downloading DrugBank file...")
        if 'DRUGBANK_USER' in environ.keys() and \
                'DRUGBANK_PWD' in environ.keys():
            r = requests.get(self._data_url,
                             auth=HTTPBasicAuth(environ['DRUGBANK_USER'],
                                                environ['DRUGBANK_PWD'])
                             )
            if r.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(r.content))
                temp_dir = self._data_path / 'temp_drugbank'
                zip_file.extractall(temp_dir)
                temp_file = temp_dir / 'full database.xml'
                db_xml_file = self._data_path / f"drugbank_{self._version}.xml"
                shutil.move(temp_file, db_xml_file)
                shutil.rmtree(temp_dir)
            else:
                if r.status_code == 401:
                    logger.error("Lacks valid DrugBank authentication "
                                 "credentials.")
                    raise requests.HTTPError("401 Unauthorized")
                logger.error("DrugBank download failed with status code:"
                             f" {r.status_code}.")
                raise requests.HTTPError(r.status_code)
        else:
            logger.error('Must enter credentials to download DrugBank '
                         'database.')
            raise KeyError("Must have environment variables DRUGBANK_USER "
                           "and DRUGBANK_PWD.")
        logger.info("Successfully downloaded DrugBank file.")

    def _extract_data(self):
        """Extract data from the DrugBank source."""
        logger.info("Extracting DrugBank file...")
        self._data_path.mkdir(exist_ok=True, parents=True)
        file_path = self._data_path / f"drugbank_{self._version}.xml"
        if not file_path.exists():
            self._download_data()
        self._data_src = file_path
        logger.info(f"Extracted {self._data_src}.")

    def _transform_data(self):
        """Transform the DrugBank source."""
        xmlns = "{http://www.drugbank.ca}"
        tree = etree.parse(f"{self._data_src}")
        root = tree.getroot()
        batch = self._database.therapies.batch_writer()
        normalizer_srcs = {
            NamespacePrefix[src].value for src in SourceName.__members__}

        for drug in root:
            params = {
                'label_and_type': None,
                'concept_id': None,
                'label': None,
                'approval_status': None,
                'aliases': [],
                'other_identifiers': [],
                'xrefs': [],
                'trade_names': [],
                'src_name': SourceName.DRUGBANK.value
            }
            for element in drug:
                # Concept ID  / Aliases
                if element.tag == f"{xmlns}drugbank-id":
                    self._load_drugbank_id(element, params)

                # Label
                if element.tag == f"{xmlns}name":
                    params['label'] = element.text

                # Aliases
                if element.tag == f"{xmlns}synonyms":
                    self._load_synonyms(element, params)
                if element.tag == f"{xmlns}international-brands":
                    self._load_international_brands(element, params, xmlns)

                # Trade Names
                if element.tag == f"{xmlns}products":
                    self._load_products(element, params, xmlns)

                # Other Identifiers
                if element.tag == f"{xmlns}external-identifiers":
                    self._load_external_identifiers(element, params, xmlns,
                                                    normalizer_srcs)
                if element.tag == f"{xmlns}cas-number":
                    self._load_cas_number(element, params)

                # Approval status
                if element.tag == f"{xmlns}groups":
                    self._load_approval_status(element, params)

            self._load_therapy(batch, params)

            if params['label']:
                self._load_label(params['label'], params['concept_id'],
                                 batch)

            if 'aliases' in params:
                if params['aliases']:
                    self._load_aliases(params['aliases'], params['concept_id'],
                                       batch)

            if 'trade_names' in params:
                if params['trade_names']:
                    self._load_trade_names(params['trade_names'],
                                           params['concept_id'], batch)

            if 'other_identifiers' in params:
                if params['other_identifiers']:
                    self._load_other_ids(params['other_identifiers'],
                                         params['concept_id'].casefold(),
                                         batch)

    def _load_therapy(self, batch, params):
        """Filter out trade names and aliases that exceed 20 and add item to
        therapies table.
        """
        if not params['other_identifiers']:
            del params['other_identifiers']
        if not params['xrefs']:
            del params['xrefs']

        for label_type in ['trade_names', 'aliases']:
            if label_type in params:
                if not params[label_type] or len(
                        {a.casefold() for a in params[label_type]}) > 20:
                    del params[label_type]
        batch.put_item(Item=params)
        self._added_ids.append(params['concept_id'])

    def _load_drugbank_id(self, element, params):
        """Load drugbank id as concept id or alias."""
        # Concept ID
        if 'primary' in element.attrib:
            params['concept_id'] = \
                f"{NamespacePrefix.DRUGBANK.value}:{element.text}"
            params['label_and_type'] = \
                f"{params['concept_id'].lower()}##identity"
        else:
            # Aliases
            params['aliases'].append(element.text)

    def _load_synonyms(self, element, params):
        """Load synonyms as aliases."""
        for alias in element:
            if alias.text not in params['aliases'] and \
                    alias.attrib['language'] == 'english':
                params['aliases'].append(alias.text)

    def _load_international_brands(self, element, params, xmlns):
        """Load international brands as aliases."""
        for international_brand in element:
            name = international_brand.find(f"{xmlns}name").text
            if name not in params['aliases']:
                params['aliases'].append(name)

    def _load_approval_status(self, element, params):
        """Load approval status."""
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

    def _load_cas_number(self, element, params):
        """Load cas number as other identifiers."""
        if element.text:
            params['other_identifiers'].append(
                f"{NamespacePrefix.CHEMIDPLUS.value}:"
                f"{element.text}")

    def _load_external_identifiers(self, element, params, xmlns,
                                   normalizer_srcs):
        """Load external identifiers as other identifiers."""
        for external_identifier in element:
            src = external_identifier.find(f"{xmlns}resource").text
            identifier = external_identifier.find(
                f"{xmlns}identifier").text
            if src in DRUGBANK_IDENTIFIER_PREFIXES.keys():
                if DRUGBANK_IDENTIFIER_PREFIXES[src] in normalizer_srcs:
                    params['other_identifiers'].append(
                        f"{DRUGBANK_IDENTIFIER_PREFIXES[src]}:"
                        f"{identifier}")
                else:
                    params['xrefs'].append(
                        f"{DRUGBANK_IDENTIFIER_PREFIXES[src]}:"
                        f"{identifier}")

    def _load_products(self, element, params, xmlns):
        """Load products as trade names."""
        for product in element:
            name = product.find(f"{xmlns}name").text
            generic = product.find(f"{xmlns}generic").text
            approved = product.find(f"{xmlns}approved").text
            over_the_counter = \
                product.find(f"{xmlns}over-the-counter").text

            if generic == "true" or approved == "true" or \
                    over_the_counter == "true":
                if name not in params['trade_names']:
                    params['trade_names'].append(name)

    def _load_label(self, label, concept_id, batch):
        """Insert label data into the database."""
        label = {
            'label_and_type':
                f"{label.lower()}##label",
            'concept_id': f"{concept_id.lower()}",
            'src_name': SourceName.DRUGBANK.value
        }
        batch.put_item(Item=label)

    def _load_aliases(self, aliases, concept_id, batch):
        """Insert alias data into the database."""
        aliases = list(set({a.casefold(): a for a in aliases}.values()))
        for alias in aliases:
            alias = {
                'label_and_type': f"{alias.lower()}##alias",
                'concept_id': f"{concept_id.lower()}",
                'src_name': SourceName.DRUGBANK.value
            }
            batch.put_item(Item=alias)

    def _load_trade_names(self, trade_names, concept_id, batch):
        """Insert trade_name data into the database."""
        trade_names = \
            list(set({t.casefold(): t for t in trade_names}.values()))
        for trade_name in trade_names:
            trade_name = {
                'label_and_type': f"{trade_name.lower()}##trade_name",
                'concept_id': f"{concept_id.lower()}",
                'src_name': SourceName.DRUGBANK.value
            }
            batch.put_item(Item=trade_name)

    def _load_other_ids(self, other_ids, concept_id, batch):
        """Insert other_id references into the database."""
        other_ids_l = {i.casefold() for i in other_ids}
        for other_id in other_ids_l:
            item = {
                'label_and_type': f'{other_id}##other_id',
                'concept_id': concept_id,
                'src_name': SourceName.DRUGBANK.value,
            }
            batch.put_item(Item=item)

    def _load_meta(self):
        """Add DrugBank metadata."""
        meta = Meta(data_license='CC BY-NC 4.0',
                    data_license_url='https://creativecommons.org/licenses/by-nc/4.0/legalcode',  # noqa: E501
                    version=self._version,
                    data_url=self._data_url,
                    rdp_url='http://reusabledata.org/drugbank.html',
                    data_license_attributes={
                        'non_commercial': True,
                        'share_alike': False,
                        'attribution': True
                    })
        params = dict(meta)
        params['src_name'] = SourceName.DRUGBANK.value
        self._database.metadata.put_item(Item=params)
