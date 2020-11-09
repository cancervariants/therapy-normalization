"""This module defines the ChEMBL ETL methods."""
from .base import Base
from therapy import PROJECT_ROOT
from ftplib import FTP
import logging
import tarfile  # noqa: F401
from therapy import database, models  # noqa: F401
from therapy.schemas import SourceName, NamespacePrefix, ApprovalStatus
from therapy.database import Base as B, engine, SessionLocal  # noqa: F401
from sqlalchemy import create_engine, event  # noqa: F401
import json

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class ChEMBL(Base):
    """ETL the ChEMBL source into therapy.db."""

    def _extract_data(self, *args, **kwargs):
        """Extract data from the ChEMBL source."""
        if 'data_path' in kwargs:
            chembl_db = kwargs['data_path']
        else:
            chembl_db = PROJECT_ROOT / 'data' / 'chembl' / 'chembl_27.db'
        if not chembl_db.exists():
            raise FileNotFoundError  # TODO: update download methods
            # chembl_archive = PROJECT_ROOT / 'data' / \
            #   'chembl_27_sqlite.tar.gz'
            # chembl_archive.parent.mkdir(exist_ok=True)
            # self._download_chembl_27(chembl_archive)
            # tar = tarfile.open(chembl_archive)
            # tar.extractall()
            # tar.close()
        assert chembl_db.exists()

    def _transform_data(self, *args, **kwargs):
        """Transform the ChEMBL source."""
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

        engine.execute("DROP TABLE test;")
        engine.execute("DROP TABLE DictionarySynonyms;")
        engine.execute("DROP TABLE TradeNames;")

        create_dictionary_synonyms_view = f"""
            CREATE TABLE DictionarySynonyms AS
            SELECT
                md.molregno,
                '{NamespacePrefix.CHEMBL.value}:'||md.chembl_id as chembl_id,
                md.pref_name,
                md.max_phase,
                md.withdrawn_flag,
                REPLACE(group_concat(
                    DISTINCT ms.synonyms), ',', '||') as synonyms
            FROM chembldb.molecule_dictionary md
            LEFT JOIN chembldb.molecule_synonyms ms USING(molregno)
            GROUP BY md.molregno
            UNION ALL
            SELECT
                md.molregno,
                '{NamespacePrefix.CHEMBL.value}:'||md.chembl_id as chembl_id,
                md.pref_name,
                md.max_phase,
                md.withdrawn_flag,
                REPLACE(group_concat(
                    DISTINCT ms.synonyms), ',', '||') as synonyms
            FROM chembldb.molecule_synonyms ms
            LEFT JOIN chembldb.molecule_dictionary md USING(molregno)
            WHERE md.molregno IS NULL
            GROUP BY md.molregno
        """
        engine.execute(create_dictionary_synonyms_view)

        create_trade_names_view = """
            CREATE TABLE TradeNames AS
            SELECT
                f.molregno,
                f.product_id,
                REPLACE(group_concat(
                    DISTINCT p.trade_name), ',', '||') as trade_names
            FROM chembldb.formulations f
            LEFT JOIN chembldb.products p
                ON f.product_id = p.product_id
            GROUP BY f.molregno;
        """
        engine.execute(create_trade_names_view)

        create_test_table = """
                    CREATE TABLE test(concept_id, label, approval_status,
                                      src_name, trade_names, aliases);
                """
        engine.execute(create_test_table)

        insert_test = f"""
            INSERT INTO test(concept_id, label, approval_status, src_name,
                             trade_names, aliases)
            SELECT
                ds.chembl_id,
                ds.pref_name,
                CASE
                    WHEN ds.withdrawn_flag
                        THEN '{ApprovalStatus.WITHDRAWN.value}'
                    WHEN ds.max_phase == 4
                        THEN '{ApprovalStatus.APPROVED.value}'
                    WHEN ds.max_phase == 0
                        THEN NULL
                    ELSE
                        '{ApprovalStatus.INVESTIGATIONAL.value}'
                END,
                '{SourceName.CHEMBL.value}',
                t.trade_names,
                ds.synonyms
            FROM DictionarySynonyms ds
            LEFT JOIN TradeNames t USING(molregno)
            GROUP BY ds.molregno
            UNION ALL
            SELECT
                ds.chembl_id,
                ds.pref_name,
                CASE
                    WHEN ds.withdrawn_flag
                        THEN '{ApprovalStatus.WITHDRAWN.value}'
                    WHEN ds.max_phase == 4
                        THEN '{ApprovalStatus.APPROVED.value}'
                    WHEN ds.max_phase == 0
                        THEN NULL
                    ELSE
                        '{ApprovalStatus.INVESTIGATIONAL.value}'
                END,
                '{SourceName.CHEMBL.value}',
                t.trade_names,
                ds.synonyms
            FROM TradeNames t
            LEFT JOIN DictionarySynonyms ds USING(molregno)
            WHERE ds.molregno IS NULL
            GROUP BY ds.molregno;
        """
        engine.execute(insert_test)

        a = """
            SELECT
                concept_id,
                label,
                approval_status,
                src_name,
                trade_names,
                aliases
            FROM test;
        """

        with open('data/chembl/chembl.json', 'w') as f:
            b = [dict(x) for x in engine.execute(a).fetchall()]
            f.write(json.dumps(b))

    def _load_data(self, *args, **kwargs):
        """Load the ChEMBL source into therapy.db."""
        B.metadata.create_all(bind=engine)
        self._get_db()
        self._add_meta()
        self._extract_data()
        self._transform_data()

    def _get_db(self, *args, **kwargs):
        """Create a new SQLAlchemy session that will be used in a single
        request, and then close it once the request is finished.
        """
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def _add_meta(self, *args, **kwargs):
        """Add ChEMBL metadata."""
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
        """Download the ChEMBL source."""
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
