"""ETL methods for the Drugs@FDA source."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.schemas import SourceMeta, SourceName, NamespacePrefix, \
    ApprovalStatus
import logging
from pathlib import Path
import requests
import zipfile
from io import BytesIO
import json
import shutil

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class DrugsAtFDA(Base):
    """Core Drugs@FDA import class."""

    def __init__(self,
                 database,
                 data_path: Path = PROJECT_ROOT / 'data',
                 src_url: str = 'https://download.open.fda.gov/drug/drugsfda/drug-drugsfda-0001-of-0001.json.zip'):  # noqa: E501
        """Initialize ETL class.
        :param Database database: DB instance to use
        :param Path data_path: path to Drugs@FDA source data folder
        :param str src_url: URL to retrieve data from
        """
        super().__init__(database, data_path)
        self._src_url = src_url

    def _download_data(self):
        """Download source data from instance-provided source URL."""
        r = requests.get(self._src_url)
        if r.status_code == 200:
            zip_file = zipfile.ZipFile(BytesIO(r.content))
        else:
            msg = f'Drugs@FDA download failed with status code: {r.status_code}'  # noqa: E501
            logger.error(msg)
            raise requests.HTTPError(r.status_code)

        orig_fname = 'drug-drugsfda-0001-of-0001.json'
        tmp_file = json.loads(zip_file.read(orig_fname))
        version = tmp_file['meta']['last_updated'].replace('-', '')
        zip_file.extract(member=orig_fname, path=self._src_data_dir)
        outfile_path = self._src_data_dir / f'drugsatfda_{version}.json'
        shutil.move(self._src_data_dir / orig_fname, outfile_path)

    def _load_meta(self):
        """Add Drugs@FDA metadata."""
        meta = {
            'data_license': 'CC0',
            'data_license_url': 'https://creativecommons.org/publicdomain/zero/1.0/legalcode',  # noqa: E501
            'version': self._version,
            'data_url': self._src_url,
            'rdp_url': None,
            'data_license_attributes': {
                'non_commercial': False,
                'share_alike': False,
                'attribution': False,
            }
        }
        assert SourceMeta(**meta)
        meta['src_name'] = SourceName.DRUGSATFDA
        self.database.metadata.put_item(Item=meta)

    def _transform_data(self):
        """Prepare source data for loading into DB."""
        with open(self._src_file, 'r') as f:
            data = json.load(f)['results']
        for result in data:
            therapy = {}
            if 'submissions' not in result:
                if 'products' not in result:
                    continue
                therapy['concept_id'] = f'{NamespacePrefix.DRUGSATFDA.value}:{result["application_number"]}'  # noqa: E501
                products = result['products']
                if not all([p['marketing_status'] == products[0]['marketing_status'] for p in products]):  # noqa: E501
                    continue
                status = products[0]['marketing_status']
                if status == 'Discontinued':
                    therapy['approval_status'] = ApprovalStatus.WITHDRAWN.value
                else:
                    therapy['approval_status'] = ApprovalStatus.APPROVED.value

                # print(result['application_number'])
                # for p in result['products']:
                #     print((p['brand_name'], p['marketing_status']))
                #  print(result)
