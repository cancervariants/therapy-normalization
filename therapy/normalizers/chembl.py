"""This module defines the ChEMBL normalizer"""
from .base import Base
from therapy import PROJECT_ROOT
from ftplib import FTP
import logging
import sqlite3
import tarfile

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class ChEMBL(Base):
    """A normalizer using the ChEMBL resource."""

    def normalize(self, query):
        """Normalize term using ChEMBL."""
        raise NotImplementedError

    def _load_data(self, *args, **kwargs):
        chembl_db = PROJECT_ROOT / 'data' / 'chembl_27' \
            / 'chembl_27_sqlite' / 'chembl_27.db'
        if not chembl_db.exists():
            chembl_archive = PROJECT_ROOT / 'data' / 'chembl_27_sqlite.tar.gz'
            chembl_archive.parent.mkdir(exist_ok=True)
            self._download_chembl_27(chembl_archive)
            tar = tarfile.open(chembl_archive)
            tar.extractall()
            tar.close()
        assert chembl_db.exists()
        conn = sqlite3.connect(chembl_db)
        self._conn = conn
        self._cursor = conn.cursor()

    # TODO: create lookup routines for normalizer

    @staticmethod
    def _download_chembl_27(filepath):
        logger.info('Downloading ChEMBL v27, this will take a few minutes.')
        try:
            with FTP('ftp.ebi.ac.uk') as ftp:
                ftp.login()
                logger.debug('FTP login completed.')
                ftp.cwd('pub/databases/chembl/ChEMBLdb/releases/chembl_27')
                with open(filepath, 'wb') as fp:
                    ftp.retrbinary('RETR chembl_27_sqlite.tar.gz', fp.write)
        except TimeoutError:
            logger.error('Connection to EBI FTP server timed out.')
