"""This module defines the ChEMBL ETL methods."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.schemas import SourceName, NamespacePrefix, ApprovalStatus, \
    SourceMeta
import logging
import tarfile
import sqlite3
import os
import shutil

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class ChEMBL(Base):
    """ETL the ChEMBL source into therapy.db."""

    def _extract_data(self, *args, **kwargs):
        """Extract data from the ChEMBL source."""
        logger.info('Extracting chembl_27.db...')
        if 'data_path' in kwargs:
            chembl_db = kwargs['data_path']
        else:
            chembl_db = self._src_data_dir / 'chembl_27.db'
        if not chembl_db.exists():
            chembl_archive = self._src_data_dir / 'chembl_27_sqlite.tar.gz'
            chembl_archive.parent.mkdir(exist_ok=True, parents=True)
            self._download_data()
            tar = tarfile.open(chembl_archive)
            tar.extractall(path=PROJECT_ROOT / 'data' / 'chembl')
            tar.close()

            # Remove unused directories and files
            chembl_27_dir = self._src_data_dir / 'chembl_27'
            temp_chembl = chembl_27_dir / 'chembl_27_sqlite' / 'chembl_27.db'
            chembl_db = self._src_data_dir / 'chembl_27.db'
            shutil.move(temp_chembl, chembl_db)
            os.remove(chembl_archive)
            shutil.rmtree(chembl_27_dir)
        conn = sqlite3.connect(chembl_db)
        conn.row_factory = sqlite3.Row
        self._conn = conn
        self._cursor = conn.cursor()
        assert chembl_db.exists()
        logger.info('Finished extracting chembl_27.db.')

    def _download_data(self, *args, **kwargs):
        """Download ChEMBL data from FTP."""
        logger.info(
            'Downloading ChEMBL v27, this will take a few minutes.')
        self._ftp_download('ftp.ebi.ac.uk',
                           'pub/databases/chembl/ChEMBLdb/releases/chembl_27',
                           self._src_data_dir,
                           'chembl_27_sqlite.tar.gz')

    def _transform_data(self, *args, **kwargs):
        """Transform SQLite data to JSON."""
        self._create_dictionary_synonyms_table()
        self._create_trade_names_table()
        self._create_temp_table()

        self._cursor.execute("DROP TABLE DictionarySynonyms;")
        self._cursor.execute("DROP TABLE TradeNames;")

        self._load_json()
        self._conn.commit()
        self._conn.close()

    def _create_dictionary_synonyms_table(self):
        """Create temporary table to store drugs and their synonyms."""
        create_dictionary_synonyms_table = f"""
            CREATE TEMPORARY TABLE DictionarySynonyms AS
            SELECT
                md.molregno,
                '{NamespacePrefix.CHEMBL.value}:'||md.chembl_id as chembl_id,
                md.pref_name,
                md.max_phase,
                md.withdrawn_flag,
                group_concat(
                    ms.synonyms, '||')as synonyms
            FROM molecule_dictionary md
            LEFT JOIN molecule_synonyms ms USING(molregno)
            GROUP BY md.molregno
            UNION ALL
            SELECT
                md.molregno,
                '{NamespacePrefix.CHEMBL.value}:'||md.chembl_id as chembl_id,
                md.pref_name,
                md.max_phase,
                md.withdrawn_flag,
                group_concat(
                    ms.synonyms, '||') as synonyms
            FROM molecule_synonyms ms
            LEFT JOIN molecule_dictionary md USING(molregno)
            WHERE md.molregno IS NULL
            GROUP BY md.molregno
        """
        self._cursor.execute(create_dictionary_synonyms_table)

    def _create_trade_names_table(self):
        """Create temporary table to store trade name data."""
        create_trade_names_table = """
            CREATE TEMPORARY TABLE TradeNames AS
            SELECT
                f.molregno,
                f.product_id,
                group_concat(
                    p.trade_name, '||') as trade_names
            FROM formulations f
            LEFT JOIN products p
                ON f.product_id = p.product_id
            GROUP BY f.molregno;
        """
        self._cursor.execute(create_trade_names_table)

    def _create_temp_table(self):
        """Create temporary table to store therapies data."""
        create_temp_table = """
            CREATE TEMPORARY TABLE temp(concept_id, label, approval_status,
                                        src_name, trade_names, aliases);
        """
        self._cursor.execute(create_temp_table)

        insert_temp = f"""
            INSERT INTO temp(concept_id, label, approval_status, src_name,
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
        self._cursor.execute(insert_temp)

    def _load_json(self):
        """Load ChEMBL data into database."""
        chembl_data = """
            SELECT
                concept_id,
                label,
                approval_status,
                src_name,
                trade_names,
                aliases
            FROM temp;
        """
        result = [dict(row) for row in
                  self._cursor.execute(chembl_data).fetchall()]
        self._cursor.execute("DROP TABLE temp;")

        for record in result:
            for attr in ['aliases', 'trade_names']:
                if attr in record and record[attr]:
                    record[attr] = record[attr].split('||')
            self._load_therapy(record)

    def _load_meta(self, *args, **kwargs):
        """Add ChEMBL metadata."""
        metadata = SourceMeta(data_license='CC BY-SA 3.0',
                              data_license_url='https://creativecommons.org/licenses/by-sa/3.0/',  # noqa: E501
                              version='27',
                              data_url='http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/',  # noqa: E501
                              rdp_url='http://reusabledata.org/chembl.html',
                              data_license_attributes={
                                  'non_commercial': False,
                                  'share_alike': True,
                                  'attribution': True
                              })
        params = dict(metadata)
        params['src_name'] = SourceName.CHEMBL.value
        self.database.metadata.put_item(Item=params)
