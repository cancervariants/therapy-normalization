"""This module defines the ChEMBL normalizer"""
from .base import Base, MatchType
from therapy import PROJECT_ROOT
from therapy.models import Drug
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
        if query.startswith('chembl:'):
            records = self._query_molecules(query.replace('chembl:', ''), 'molecule_dictionary',
                                            'chembl_id')
        else:
            records = self._query_molecules(query, 'molecule_dictionary',
                                            'chembl_id')
        if records:
            return self.NormalizerResponse(MatchType.PRIMARY, records)
        records = self._query_molecules(query, 'molecule_dictionary',
                                        'pref_name')
        if records:
            return self.NormalizerResponse(MatchType.PRIMARY, records)
        records = self._query_molecules(query, 'molecule_dictionary',
                                        'pref_name', lower=True)
        if records:
            return self.NormalizerResponse(MatchType.CASE_INSENSITIVE_PRIMARY,
                                           records)
        records = self._query_molecules(query, 'molecule_synonyms',
                                        'synonyms')
        if records:
            return self.NormalizerResponse(MatchType.ALIAS, records)
        records = self._query_molecules(query, 'molecule_synonyms',
                                        'synonyms', lower=True)
        if records:
            return self.NormalizerResponse(MatchType.CASE_INSENSITIVE_ALIAS,
                                           records)
        return self.NormalizerResponse(MatchType.NO_MATCH, tuple())

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
        conn = sqlite3.connect(chembl_db, check_same_thread=False)
        self._conn = conn
        self._cursor = conn.cursor()
        self._create_lower_index_if_not_exists('molecule_dictionary',
                                               'pref_name')
        self._create_lower_index_if_not_exists('molecule_synonyms',
                                               'synonyms')

    def _query_molecules(self, query, table, field, lower=False):
        if lower:
            command = f"""
                SELECT molregno FROM {table}
                WHERE {field}='{query}';
            """
        else:
            command = f"""
                SELECT molregno FROM {table}
                WHERE lower({field})='{query.lower()}';
            """
        molregno_list = [x[0] for x
                         in self._cursor.execute(command).fetchall()]
        records = [self._create_drug_record(molregno) for molregno
                   in molregno_list]
        return records

    def _create_drug_record(self, molregno):
        # select relevant fields from molecular_dictionary
        command = f"""
            SELECT chembl_id, pref_name, max_phase, withdrawn_flag
            FROM molecule_dictionary
            WHERE molregno={molregno}
        """
        resp = self._cursor.execute(command).fetchall()
        assert len(resp) == 1
        result = resp[0]
        kwargs = {
            'concept_identifier': f'chembl:{result[0]}',
            'label': result[1],
            'max_phase': result[2],
            'withdrawn': result[3],
            'other_identifiers': list()
        }
        command = f"""
            SELECT synonyms FROM molecule_synonyms
            WHERE molregno={molregno}
        """
        resp = self._cursor.execute(command).fetchall()
        synonyms = tuple(set([x[0] for x in resp]))
        kwargs['aliases'] = synonyms
        d = Drug(**kwargs)
        # create a Drug object from response
        return d

    def _create_lower_index_if_not_exists(self, table, field):
        try:
            command = f"""
                CREATE INDEX lower_{field} ON {table} (lower({field}));
            """
            self._cursor.execute(command)
        except sqlite3.OperationalError as e:
            if str(e) == f'index lower_{field} already exists':
                return False
            else:
                raise e
        return True

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
