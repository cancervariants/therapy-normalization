"""ETL methods for ChemIDPlus source.
TODO?
 * Speed up
 * restrict alias from equalling label/etc?
 * include extra 'label'ish values as additional aliases
"""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.database import Database
from therapy.schemas import Drug, NamespacePrefix, Meta, SourceName, \
    DataLicenseAttributes
from pathlib import Path
import xml.etree.ElementTree as ET
import requests
import logging
from typing import Dict
from boto3.dynamodb.table import BatchWriter
import re


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


TAGS_REGEX = r' \[.*\]'


class ChemIDplus(Base):
    """Core ChemIDplus ETL class."""

    def __init__(self,
                 database: Database,
                 data_path: Path = PROJECT_ROOT / 'data' / 'chemidplus',
                 src_dir: str = 'ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/',  # noqa: E501
                 src_fname: str = 'CurrentChemID.xml'):
        """Initialize class instance.

        :param therapy.database.Database database: database instance to use
        :param Path data_path: path to chemidplus subdirectory in application
            data folder
        :param str src_dir: URL to remote directory containing source file
        :param str src_fname: name of file as stored in src_dir.

        If the source file is provided locally in the data_path directory,
        it's unnecessary to provide `src_dir` and `src_fname` args.
        """
        self.database = database
        self._src_dir = src_dir
        self._src_fname = src_fname
        self._added_ids = set()
        # perform ETL
        self.added_num = 0
        self._extract_data(data_path)
        self._transform_data()
        self._add_meta()

    def _download_data(self):
        """Download source data from default location."""
        logger.info('Downloading ChemIDplus data...')
        url = self._src_dir + self._src_fname
        out_path = PROJECT_ROOT / 'data' / 'chemidplus' / self._src_fname
        response = requests.get(url, stream=True)
        handle = open(out_path, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                handle.write(chunk)
        version = ET.parse(out_path).getroot().attrib['date'].replace('-', '')
        out_path.rename(f'chemidplus_{version}.xml')
        logger.info('Finished downloading ChemIDplus data')

    def _extract_data(self, data_dir: Path):
        """Acquire ChemIDplus dataset.

        :arg pathlib.Path data_dir: directory containing source data
        """
        data_dir.mkdir(exist_ok=True, parents=True)
        dir_files = list(data_dir.iterdir())
        if len(dir_files) == 0:
            self._download_data()
            dir_files = list(data_dir.iterdir())
        file = sorted([f for f in dir_files
                       if f.name.startswith('chemidplus')])
        self._data_src = file[-1]
        self._version = self._data_src.stem.split('_')[1]

    def _transform_data(self):
        """Open dataset and prepare for loading into database."""
        tree = ET.parse(self._data_src)
        root = tree.getroot()
        with self.database.therapies.batch_writer() as batch:
            for chemical in root:
                if 'displayName' not in chemical.attrib:
                    continue

                # initial setup and get label
                display_name = chemical.attrib['displayName']
                if not display_name or not re.search(TAGS_REGEX, display_name):
                    continue
                label = re.sub(TAGS_REGEX, '', display_name)
                params = {
                    'label': label
                }

                # get concept ID
                reg_no = chemical.find('NumberList').find("CASRegistryNumber")
                if not reg_no:
                    continue
                params['concept_id'] = f'{NamespacePrefix.CASREGISTRY.value}:{reg_no.text}'  # noqa: E501

                # get aliases
                aliases = []
                label_l = label.lower()
                name_list = chemical.find('NameList')
                if name_list:
                    for name in name_list.findall('NameOfSubstance'):
                        text = name.text
                        if text != display_name and text.lower() != label_l:
                            aliases.append(text)
                params['aliases'] = aliases

                # get other_ids and xrefs
                params['other_identifiers'] = list()
                params['xrefs'] = list()
                locator_list = chemical.find('LocatorList')
                if locator_list:
                    for loc in locator_list.findall('InternetLocator'):
                        if loc.text == 'DrugBank':
                            db = f'{NamespacePrefix.DRUGBANK.value}:{loc.attrib["url"].split("/")[-1]}'  # noqa: E501
                            params['other_identifiers'].append(db)
                        elif loc.text == 'FDA SRS':
                            fda = f'{NamespacePrefix.FDA.value}:{loc.attrib["url"].split("/")[-1]}'  # noqa: E501
                            params['xrefs'].append(fda)

                # double-check and load full record
                assert Drug(**params)
                self._load_record(batch, params)

    def _load_record(self, batch: BatchWriter, record: Dict):
        """Load individual record into database.

        :param boto3.dynamodb.table.BatchWriter batch: dynamodb batch writer
            instance
        :param therapy.schemas.Drug record: complete drug record to upload
        """
        concept_id_l = record['concept_id'].lower()
        for alias in {a.lower() for a in record['aliases']}:
            batch.put_item(Item={
                'label_and_type': f'{alias}##alias',
                'concept_id': concept_id_l,
                'src_name': SourceName.CHEMIDPLUS.value
            })
        if 'label' in record:
            batch.put_item(Item={
                'label_and_type': f'{record["label"].lower()}##label',
                'concept_id': concept_id_l,
                'src_name': SourceName.CHEMIDPLUS.value
            })
        record['src_name'] = SourceName.CHEMIDPLUS.value
        record['label_and_type'] = f'{concept_id_l}##identity'
        batch.put_item(Item=record)
        self._added_ids.add(record['concept_id'])

    def _add_meta(self):
        """Add source metadata."""
        meta = Meta(data_license="custom",
                    data_license_url="https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                    version=self._version,
                    data_url="ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                    rdp_url=None,
                    data_license_attributes=DataLicenseAttributes(
                        non_commercial=False,
                        share_alike=False,
                        attribution=True
                    ))
        item = dict(meta)
        item['src_name'] = SourceName.CHEMIDPLUS.value
        self.database.metadata.put_item(Item=item)
