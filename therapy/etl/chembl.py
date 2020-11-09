"""This module defines the ChEMBL ETL methods."""
import os

from .base import Base
from therapy import PROJECT_ROOT
from ftplib import FTP
import logging
import tarfile  # noqa: F401
from therapy import database, models  # noqa: F401
from therapy.schemas import SourceName, NamespacePrefix, ApprovalStatus
import json
import sqlite3

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
        conn = sqlite3.connect(chembl_db)
        conn.row_factory = sqlite3.Row
        self._conn = conn
        self._cursor = conn.cursor()
        assert chembl_db.exists()

    def _transform_data(self, *args, **kwargs):
        """Transform the ChEMBL source."""
        create_dictionary_synonyms_table = f"""
            CREATE TEMPORARY TABLE DictionarySynonyms AS
            SELECT
                md.molregno,
                '{NamespacePrefix.CHEMBL.value}:'||md.chembl_id as chembl_id,
                md.pref_name,
                md.max_phase,
                md.withdrawn_flag,
                REPLACE(group_concat(
                    DISTINCT ms.synonyms), ',', '||') as synonyms
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
                REPLACE(group_concat(
                    DISTINCT ms.synonyms), ',', '||') as synonyms
            FROM molecule_synonyms ms
            LEFT JOIN molecule_dictionary md USING(molregno)
            WHERE md.molregno IS NULL
            GROUP BY md.molregno
        """
        self._cursor.execute(create_dictionary_synonyms_table)

        create_trade_names_table = """
            CREATE TEMPORARY TABLE TradeNames AS
            SELECT
                f.molregno,
                f.product_id,
                REPLACE(group_concat(
                    DISTINCT p.trade_name), ',', '||') as trade_names
            FROM formulations f
            LEFT JOIN products p
                ON f.product_id = p.product_id
            GROUP BY f.molregno;
        """
        self._cursor.execute(create_trade_names_table)

        create_temp_table = """
            CREATE TEMPORARY TABLE temp(concept_id, label, approval_status,
                                        src_name, trade_names, aliases);
        """
        self._cursor.execute(create_temp_table)

        insert_test = f"""
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
        self._cursor.execute(insert_test)

    def _load_json(self):
        """Load ChEMBL data into JSON file."""
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
        with open('data/chembl/chembl_temp.json', 'w+') as temp:
            result = [dict(row) for row in
                      self._cursor.execute(chembl_data).fetchall()]
            temp.write(json.dumps(result))
            temp.seek(0)

            records = json.load(temp)

            records_list = []
            with open('data/chembl/chembl.json', 'w') as f:
                for record in records:
                    if record['aliases']:
                        record['aliases'] = record['aliases'].split("||")
                    else:
                        record['aliases'] = list()
                    if record['trade_names']:
                        record['trade_names'] = \
                            record['trade_names'].split("||")
                    else:
                        record['trade_names'] = list()
                    records_list.append(record)
                f.write(json.dumps(records_list))
            os.remove(temp.name)

        self._cursor.execute("DROP TABLE temp;")
        self._cursor.execute("DROP TABLE DictionarySynonyms;")
        self._cursor.execute("DROP TABLE TradeNames;")

    def _load_data(self, *args, **kwargs):
        """Load the ChEMBL source into therapy.db."""
        self._add_meta()  # TODO
        self._extract_data()
        self._transform_data()
        self._load_json()
        self._conn.commit()
        self._conn.close()

    def _add_meta(self, *args, **kwargs):
        """Add ChEMBL metadata."""
        # insert_meta = f"""
        #     INSERT INTO meta_data(src_name, data_license, data_license_url,
        #         version, data_url)
        #     SELECT
        #         '{SourceName.CHEMBL.value}',
        #         'CC BY-SA 3.0',
        #         'https://creativecommons.org/licenses/by-sa/3.0/',
        #         '27',
        #         'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/'
        #     WHERE NOT EXISTS (
        #         SELECT * FROM meta_data
        #         WHERE src_name = '{SourceName.CHEMBL.value}'
        #     );
        # """
        pass

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
