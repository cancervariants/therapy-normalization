"""ETL methods for ChemIDPlus® source.

Courtesy of the U.S. National Library of Medicine.
"""
from .base import Base
from typing import List, Dict
from therapy import PROJECT_ROOT
from therapy.schemas import Drug, NamespacePrefix, SourceMeta, SourceName, \
    DataLicenseAttributes
from pathlib import Path
from ftplib import FTP
import xml.etree.ElementTree as ET
import logging
from boto3.dynamodb.table import BatchWriter
import re


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


TAGS_REGEX = r' \[.*\]'


class ChemIDplus(Base):
    """Core ChemIDplus ETL class."""

    def __init__(self,
                 database,
                 data_path: Path = PROJECT_ROOT / 'data' / 'chemidplus',
                 src_server: str = 'ftp.nlm.nih.gov',
                 src_dir_path: str = 'nlmdata/.chemidlease/',
                 src_fname: str = 'CurrentChemID.xml'):
        """Initialize class instance.

        :param therapy.database.Database database: database instance to use
        :param Path data_path: path to chemidplus subdirectory in application
            data folder
        :param str src_server: The NLM domain
        :param str src_dir_path: The directory to the chemidplus release
        :param str src_fname: name of file as stored in src_dir.

        If the source file is provided locally in the data_path directory,
        it's unnecessary to provide `src_dir` and `src_fname` args.
        """
        self.database = database
        self._data_path = data_path
        self._src_server = src_server
        self._src_dir_path = src_dir_path
        self._src_fname = src_fname
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

    def _download_data(self, data_path: Path):
        """Download source data from default location."""
        logger.info('Downloading ChemIDplus data...')
        outfile_path = data_path / self._src_fname
        try:
            with FTP(self._src_server) as ftp:
                ftp.login()
                logger.debug('FTP login successful.')
                ftp.cwd(self._src_dir_path)
                with open(outfile_path, 'wb') as fp:
                    ftp.retrbinary(f'RETR {self._src_fname}', fp.write)
            logger.info('Downloaded ChemIDplus source file.')
        except TimeoutError:
            logger.error('Connection to EBI FTP server timed out.')

        parser = ET.iterparse(outfile_path, ('start', 'end'))
        date = next(parser)[1].attrib['date']
        version = date.replace('-', '')
        outfile_path.rename(data_path / f'chemidplus_{version}.xml')
        logger.info('Finished downloading ChemIDplus data')

    def _extract_data(self):
        """Acquire ChemIDplus dataset.

        :arg pathlib.Path data_path: directory containing source data
        """
        self._data_path.mkdir(exist_ok=True, parents=True)
        dir_files = list(self._data_path.iterdir())

        if len(dir_files) == 0:
            file = self._get_file(self._data_path)
        else:
            file = sorted([f for f in dir_files
                           if f.name.startswith('chemidplus')])
            if not file:
                file = self._get_file(self._data_path)

        self._data_src = file[-1]
        self._version = self._data_src.stem.split('_')[1]

    def _get_file(self, data_dir):
        self._download_data(self._data_path)
        dir_files = list(data_dir.iterdir())
        return sorted([f for f in dir_files
                       if f.name.startswith('chemidplus')])

    @staticmethod
    def parse_xml(path: Path, tag: str):
        """Parse XML file and retrieve elements with matching tag value.
        :param Path path: path to XML file
        :param str tag: XML tag
        :return: generator yielding elements of corresponding tag
        """
        context = iter(ET.iterparse(path, events=('start', 'end')))
        _, root = next(context)
        for event, elem in context:
            if event == 'end' and elem.tag == tag:
                yield elem
                root.clear()

    def _transform_data(self):
        """Open dataset and prepare for loading into database."""
        parser = self.parse_xml(self._data_src, 'Chemical')
        with self.database.therapies.batch_writer() as batch:
            for chemical in parser:
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
                            aliases.append(re.sub(TAGS_REGEX, '', text))
                params['aliases'] = aliases

                # get xrefs and associated_with
                params['xrefs'] = []
                params['associated_with'] = []
                locator_list = chemical.find('LocatorList')
                if locator_list:
                    for loc in locator_list.findall('InternetLocator'):
                        if loc.text == 'DrugBank':
                            db = f'{NamespacePrefix.DRUGBANK.value}:{loc.attrib["url"].split("/")[-1]}'  # noqa: E501
                            params['xrefs'].append(db)
                        elif loc.text == 'FDA SRS':
                            fda = f'{NamespacePrefix.FDA.value}:{loc.attrib["url"].split("/")[-1]}'  # noqa: E501
                            params['associated_with'].append(fda)

                # double-check and load full record
                assert Drug(**params)
                self._load_record(batch, params)

    def _load_record(self, batch: BatchWriter, record: Dict):
        """Load individual record into database.

        :param boto3.dynamodb.table.BatchWriter batch: dynamodb batch writer
            instance
        :param therapy.schemas.Drug record: complete drug record to upload
        """
        concept_id_ref = record['concept_id'].lower()

        if record.get('label'):
            batch.put_item(Item={
                'label_and_type': f'{record["label"].lower()}##label',
                'concept_id': concept_id_ref,
                'src_name': SourceName.CHEMIDPLUS.value,
                'item_type': 'label',
            })
        else:
            del record['label']

        for field_type, field in (('alias', 'aliases'),
                                  ('xref', 'xrefs'),
                                  ('associated_with', 'associated_with')):
            values = record.get(field)
            if values:
                keys = {value.casefold() for value in values}
                if field == 'aliases' and len(keys) > 20:
                    del record['aliases']
                    continue
                for key in keys:
                    pk = f"{key}##{field_type}"
                    batch.put_item(Item={
                        'label_and_type': pk,
                        'concept_id': concept_id_ref,
                        'src_name': SourceName.CHEMIDPLUS.value,
                        'item_type': field_type,
                    })
            else:
                del record[field]

        record['src_name'] = SourceName.CHEMIDPLUS.value
        record['label_and_type'] = f'{concept_id_ref}##identity'
        record['item_type'] = 'identity'
        batch.put_item(Item=record)
        self._added_ids.append(record['concept_id'])

    def _load_meta(self):
        """Add source metadata."""
        meta = SourceMeta(data_license="custom",
                          data_license_url="https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                          version=self._version,
                          data_url="ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",  # noqa: E501
                          rdp_url=None,
                          data_license_attributes=DataLicenseAttributes(
                              non_commercial=False,
                              share_alike=False,
                              attribution=True
                          ))
        item = dict(meta)
        item['src_name'] = SourceName.CHEMIDPLUS.value
        self.database.metadata.put_item(Item=item)
