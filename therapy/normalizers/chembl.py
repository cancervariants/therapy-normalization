"""This module defines the ChEMBL normalizer"""
from .base import Base
from therapy import PROJECT_ROOT
from ftplib import FTP
import sqlite3


class ChEMBL(Base):
    """A normalizer using the ChEMBL resource."""

    def normalize(self, query):
        """Normalize term using ChEMBL"""
        raise NotImplementedError

    def _load_data(self, *args, **kwargs):
        wd_file = PROJECT_ROOT / 'data' / 'chembl_27_sqlite.tar.gz'
        if not wd_file.exists():
            wd_file.parent.mkdir(exist_ok=True)
            self._download_chembl_27(wd_file)
        assert wd_file.exists()
        conn = sqlite3.connect(wd_file)
        assert conn
        # TODO: parse file to create in-memory lookups

    @staticmethod
    def _download_chembl_27(filepath):
        with FTP('ftp.ebi.ac.uk') as ftp:
            ftp.login()
            with open(filepath, 'wb') as fp:
                ftp.cwd('pub/databases/chembl/ChEMBLdb/releases/chembl_27')
                ftp.retrbinary('chembl_27_sqlite.tar.gz', fp.write)
