"""ETL methods for the Drugs@FDA source."""
from .base import Base
from therapy import PROJECT_ROOT
import logging
from pathlib import Path
import requests
import zipfile
from io import BytesIO
import re


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class DrugsAtFDA(Base):
    """Core Drugs@FDA import class."""

    def __init__(self,
                 database,
                 data_path: Path = PROJECT_ROOT / 'data',
                 src_url: str = 'https://www.fda.gov/media/89850/download'):
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

        pattern = r'filename=drugsatfda(.+)\.zip'
        self._version = re.findall(pattern,
                                   r.headers['content-disposition'])[0]
        needed_files = (
            'Products.txt',
            'MarketingStatus.txt',
        )
        for file in needed_files:
            zip_file.extract(member=file, path=self._src_data_dir)

    def _extract_data(self):
        """Extract Therapy records from source data."""
        marketing_status_lookup = {
            1: 'Prescription',
            2: 'Over-the-counter',
            3: 'Discontinued',
            4: 'None (Tentative Approval)'
        }
        return marketing_status_lookup

        # Products.txt == name
        # Submissions.txt = submission
        # MarketingStatus.txt == approval status
