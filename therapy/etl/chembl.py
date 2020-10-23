"""This module defines the ChEMBL ETL methods."""
from .base import Base
from therapy import PROJECT_ROOT
from ftplib import FTP
import logging
import tarfile
from therapy import database, models  # noqa: F401
from therapy.schemas import SourceName, NamespacePrefix
from therapy.database import Base as B, engine, SessionLocal  # noqa: F401
from sqlalchemy import create_engine, event  # noqa: F401

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class ChEMBL(Base):
    """ETL ChEMBL resource into therapy.db."""

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

    def _transform_data(self, *args, **kwargs):
        @event.listens_for(engine, "connect")
        def connect(engine, rec):
            copy_chembl_db = PROJECT_ROOT / 'data' / 'chembl' / 'chembl_27.db'
            attach_db = f"""
                ATTACH DATABASE '{copy_chembl_db}' AS chembldb;
            """
            engine.execute(attach_db)
            cursor = engine.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
        engine.connect()

        insert_therapy = f"""
            INSERT INTO therapies(
                concept_id, label, max_phase, withdrawn_flag, src_name
            )
            SELECT DISTINCT
                '{NamespacePrefix.CHEMBL.value}:'||molecule_dictionary.chembl_id,
                molecule_dictionary.pref_name,
                molecule_dictionary.max_phase,
                molecule_dictionary.withdrawn_flag,
                '{SourceName.CHEMBL.value}'
            FROM chembldb.molecule_dictionary;
        """
        engine.execute(insert_therapy)

        insert_alias = f"""
            INSERT INTO aliases(alias, concept_id)
            SELECT DISTINCT
                synonyms,
                '{NamespacePrefix.CHEMBL.value}:'||chembl_id
            FROM chembldb.molecule_dictionary
            LEFT JOIN chembldb.molecule_synonyms
                ON molecule_dictionary.molregno=molecule_synonyms.molregno
            WHERE molecule_synonyms.synonyms NOT NULL;
        """
        engine.execute(insert_alias)

        insert_trade_name = f"""
            INSERT INTO trade_names(trade_name, concept_id)
            SELECT DISTINCT
                products.trade_name,
                '{NamespacePrefix.CHEMBL.value}:'||molecule_dictionary.chembl_id
            FROM chembldb.molecule_dictionary
            LEFT JOIN chembldb.formulations
                ON molecule_dictionary.molregno=formulations.molregno
            LEFT JOIN chembldb.products
                ON formulations.product_id=products.product_id
            WHERE products.trade_name NOT NULL;
        """
        engine.execute(insert_trade_name)

    def _load_data(self, *args, **kwargs):
        B.metadata.create_all(bind=engine)
        self._get_db()
        self._add_meta()
        self._extract_data()
        self._transform_data()

    def _get_db(self, *args, **kwargs):
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def _add_meta(self, *args, **kwargs):
        insert_meta = f"""
            INSERT INTO meta_data(src_name, data_license, data_license_url,
                version, data_url)
            SELECT
                '{SourceName.CHEMBL.value}',
                'CC BY-SA 3.0',
                'https://creativecommons.org/licenses/by-sa/3.0/',
                '27',
                'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'
            WHERE NOT EXISTS (
                SELECT * FROM meta_data
                WHERE src_name = '{SourceName.CHEMBL.value}'
            );
        """
        engine.execute(insert_meta)

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
