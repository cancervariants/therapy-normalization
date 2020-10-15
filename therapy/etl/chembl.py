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

    def _extract_data(self, *args, **kwargs):
        if 'data_path' in kwargs:
            chembl_db = kwargs['data_path']
        else:
            chembl_db = PROJECT_ROOT / 'data' / 'chembl' / 'chembl_27.db'
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

    def _create_tables(self, *args, **kwargs):
        # TODO: update sqlalchemy db
        create_aliases_table = """
                   CREATE TABLE IF NOT EXISTS aliases(
                       id integer PRIMARY KEY AUTOINCREMENT,
                       alias text,
                       concept_id text
                   );
               """
        self._t_cursor.execute(create_aliases_table)

        create_therapies_table = """
                           CREATE TABLE IF NOT EXISTS therapies(
                               concept_id text,
                               label text,
                               max_phase text,
                               withdrawn_flag integer,
                               trade_name text,
                               src_name text
                           );
                       """
        self._t_cursor.execute(create_therapies_table)

        create_other_identifiers_table = """
                           CREATE TABLE IF NOT EXISTS other_identifiers(
                               id integer PRIMARY KEY AUTOINCREMENT,
                               concept_id text,
                               chembl_id text,
                               wikidata_id text,
                               ncit_id text,
                               drugbank_id text
                           );
                       """
        self._t_cursor.execute(create_other_identifiers_table)
        self._t_conn.commit()

    def _transform_data(self, *args, **kwargs):
        copy_chembl_db = PROJECT_ROOT / 'data' / 'chembl' / 'chembl_27.db'
        attach_db = f"""
            ATTACH DATABASE '{copy_chembl_db}' AS chembldb;
        """
        self._t_cursor.execute(attach_db)  # TODO: Use sqlalchemy db

        get_molgrenos = """
                    SELECT DISTINCT molregno FROM chembldb.molecule_dictionary;
        """
        distinct_molregnos = \
            [x[0] for x in self._t_cursor.execute(get_molgrenos)]

        self._create_tables()

        for molregno in distinct_molregnos:
            logger.warning(molregno)
            insert_alias = f"""
                INSERT INTO aliases(alias, concept_id)
                SELECT DISTINCT synonyms, chembl_id
                FROM chembldb.molecule_dictionary
                LEFT JOIN chembldb.molecule_synonyms
                    ON molecule_dictionary.molregno=molecule_synonyms.molregno
                    WHERE molecule_dictionary.molregno={molregno};
            """
            self._t_cursor.execute(insert_alias)

            insert_therapy = f"""
                INSERT INTO therapies(
                    concept_id, label, max_phase, withdrawn_flag,
                    trade_name, src_name
                )
                SELECT DISTINCT molecule_dictionary.chembl_id,
                    molecule_dictionary.pref_name,
                    molecule_dictionary.max_phase,
                    molecule_dictionary.withdrawn_flag,
                    products.trade_name,
                    substr(molecule_dictionary.chembl_id,1,6)
                FROM chembldb.molecule_dictionary
                LEFT JOIN chembldb.formulations
                    ON molecule_dictionary.molregno=formulations.molregno
                LEFT JOIN chembldb.products
                    ON formulations.product_id=products.product_id
                WHERE molecule_dictionary.molregno={molregno};
            """
            self._t_cursor.execute(insert_therapy)

            insert_other_identifier = f"""
                INSERT INTO other_identifiers(concept_id, chembl_id)
                SELECT DISTINCT chembl_id, chembl_id
                FROM chembldb.molecule_dictionary
                WHERE molecule_dictionary.molregno={molregno};
            """
            self._t_cursor.execute(insert_other_identifier)
            self._t_conn.commit()

    def _load_data(self, *args, **kwargs):
        chembl_transformed_db = \
            PROJECT_ROOT / 'data' / 'chembl' / 'chembl_transformed.db'
        t_conn = sqlite3.connect(chembl_transformed_db)
        self._t_conn = t_conn
        self._t_cursor = t_conn.cursor()

        self._extract_data()
        self._transform_data()
        self._add_meta()

    def _add_meta(self, *args, **kwargs):
        insert_meta_table = """
                    CREATE TABLE IF NOT EXISTS meta(
                        src_name text,
                        data_license text,
                        data_license_url text,
                        version text,
                        data_url text
                    );
        """
        self._t_cursor.execute(insert_meta_table)

        insert_meta = """
            INSERT INTO meta(src_name, data_license, data_license_url,
                version, data_url)
            SELECT
                'CHEMBL',
                'CC BY-SA 3.0',
                'https://creativecommons.org/licenses/by-sa/3.0/',
                '27',
                'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'
            WHERE NOT EXISTS (
                SELECT * FROM meta
                WHERE src_name = 'CHEMBL'
            );
        """
        self._t_cursor.execute(insert_meta)
        self._t_conn.commit()

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
