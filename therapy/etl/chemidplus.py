"""ETL methods for ChemIDPlus® source.

Courtesy of the U.S. National Library of Medicine.
"""
import logging
from urllib.parse import urlparse
import zipfile
from os import remove
from pathlib import Path
import xml.etree.ElementTree as ET
import re
from typing import Dict, Any

import bioversions

from .base import Base
from therapy.schemas import Drug, NamespacePrefix, SourceMeta, SourceName, \
    DataLicenseAttributes


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


TAGS_REGEX = r' \[.*\]'


class ChemIDplus(Base):
    """Core ChemIDplus ETL class."""

    def _download_data(self):
        """Download source data from default location."""
        logger.info('Retrieving source data for ChemIDplus')
        url = urlparse(bioversions.resolve('chemidplus').homepage)
        path, file = url.path.rsplit('/', 1)
        self._ftp_download(url.netloc, path, file)
        zip_path = (self._src_data_dir / file).absolute()
        zip_file = zipfile.ZipFile(zip_path, 'r')
        outfile = self._src_data_dir / f'chemidplus_{self._version}.xml'
        for info in zip_file.infolist():
            if re.match(r'*.xml', info.filename):
                zip_file.extract(info, path=outfile)
        remove(zip_path)
        logger.info('Successfully retrieved source data for ChemIDplus')

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
        parser = self.parse_xml(self._src_file, 'Chemical')
        for chemical in parser:
            if 'displayName' not in chemical.attrib:
                continue

            # initial setup and get label
            display_name = chemical.attrib['displayName']
            if not display_name or not re.search(TAGS_REGEX, display_name):
                continue
            label = re.sub(TAGS_REGEX, '', display_name)
            params: Dict[str, Any] = {
                'label': label
            }

            # get concept ID
            reg_no = chemical.find('NumberList').find("CASRegistryNumber")
            if not reg_no:
                continue
            params['concept_id'] = \
                f'{NamespacePrefix.CASREGISTRY.value}:{reg_no.text}'

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
                        db = f'{NamespacePrefix.DRUGBANK.value}:' \
                             f'{loc.attrib["url"].split("/")[-1]}'
                        params['xrefs'].append(db)
                    elif loc.text == 'FDA SRS':
                        unii = f'{NamespacePrefix.UNII.value}:' \
                               f'{loc.attrib["url"].split("/")[-1]}'
                        params['associated_with'].append(unii)

            # double-check and load full record
            assert Drug(**params)
            self._load_therapy(params)

    def _load_meta(self):
        """Add source metadata."""
        meta = SourceMeta(data_license="custom",
                          data_license_url="https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                          version=self._version,
                          data_url=bioversions.resolve('chemidplus').homepage,
                          rdp_url=None,
                          data_license_attributes=DataLicenseAttributes(
                              non_commercial=False,
                              share_alike=False,
                              attribution=True
                          ))
        item = dict(meta)
        item['src_name'] = SourceName.CHEMIDPLUS.value
        self.database.metadata.put_item(Item=item)
