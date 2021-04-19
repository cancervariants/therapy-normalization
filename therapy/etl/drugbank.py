"""This module defines the DrugBank ETL methods."""
from therapy import DownloadException
from therapy.schemas import SourceName, SourceMeta, NamespacePrefix
from therapy.etl.base import Base
import logging
import csv
from typing import List
import bs4
import requests
import re
import zipfile
import shutil
from io import BytesIO

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class DrugBank(Base):
    """ETL the DrugBank source into therapy.db."""

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
        """Download DrugBank source data."""
        logger.info("Downloading DrugBank source data...")

        # get newest version number
        r = requests.get('https://go.drugbank.com/release_notes')
        if r.status_code == 200:
            soup = bs4.BeautifulSoup(r.content, features='lxml')
        else:
            logger.error(f'DrugBank version fetch failed with status code: '
                         f'{r.status_code}')
            raise DownloadException
        most_recent = soup.find('div', {'class': 'card-header'})
        version = re.search(r'[0-9]+\.[0-9]+\.[0-9]+',
                            most_recent.contents[0]).group()
        url = f'https://go.drugbank.com/releases/{version.replace(".", "-")}/downloads/all-drugbank-vocabulary'  # noqa: E501

        # download file
        r = requests.get(url)
        if r.status_code == 200:
            zip_file = zipfile.ZipFile(BytesIO(r.content))
        else:
            logger.error("DrugBank download failed with status code:"
                         f" {r.status_code}")
            raise requests.HTTPError(r.status_code)

        # unpack file
        temp_dir = self._src_data_dir / 'temp_drugbank'
        zip_file.extractall(temp_dir)
        temp_file = temp_dir / 'drugbank vocabulary.csv'
        csv_file = self._src_data_dir / f'drugbank_{version}.csv'
        shutil.move(temp_file, csv_file)
        shutil.rmtree(temp_dir)
        logger.info("DrugBank source data download complete.")

    def _load_meta(self):
        """Add DrugBank metadata."""
        meta = {
            'data_license': 'CC0 1.0',
            'data_license_url': 'https://creativecommons.org/publicdomain/zero/1.0/',  # noqa: E501
            'version': self._version,
            'data_url': 'https://go.drugbank.com/releases/latest#open-data',
            'rdp_url': 'http://reusabledata.org/drugbank.html',
            'data_license_attributes': {
                'non_commercial': False,
                'share_alike': False,
                'attribution': False,
            },
        }
        assert SourceMeta(**meta)
        meta['src_name'] = SourceName.DRUGBANK.value
        self.database.metadata.put_item(Item=meta)

    def _transform_data(self):
        """Transform the DrugBank source."""
        with open(self._src_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for row in reader:
                params = {
                    'concept_id': f'{NamespacePrefix.DRUGBANK.value}:{row[0]}',
                }
                label = row[2]
                if label:
                    params['label'] = label
                aliases = [
                    a for a in row[1].split(' | ') + row[5].split(' | ') if a
                ]
                if aliases:
                    params['aliases'] = aliases
                cas_ref = row[3]
                if cas_ref:
                    params['other_identifiers'] = [
                        f'{NamespacePrefix.CHEMIDPLUS.value}:{row[3]}'
                    ]
                self._load_therapy(params)
