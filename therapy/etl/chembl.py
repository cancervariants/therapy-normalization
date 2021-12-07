"""This module defines the ChEMBL ETL methods."""
import logging
import os
import shutil
import sqlite3

import chembl_downloader
import bioversions

from therapy.etl.base import Base
from therapy.schemas import SourceName, NamespacePrefix, ApprovalRating, \
    SourceMeta


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class ChEMBL(Base):
    """Class for ChEMBL ETL methods."""

    def _download_data(self) -> None:
        """Download latest ChEMBL database file from EBI."""
        logger.info("Retrieving source data for ChEMBL")
        os.environ["PYSTOW_HOME"] = str(self._src_dir.parent.absolute())
        tmp_path = chembl_downloader.download_extract_sqlite()
        shutil.move(tmp_path, self._src_dir)
        shutil.rmtree(tmp_path.parent.parent.parent)
        logger.info("Successfully retrieved source data for ChEMBL")

    def _extract_data(self) -> None:
        """Extract data from the ChEMBL source."""
        super()._extract_data()
        conn = sqlite3.connect(self._src_file)
        conn.row_factory = sqlite3.Row
        self._conn = conn
        self._cursor = conn.cursor()

    def _transform_data(self) -> None:
        """Transform SQLite data to temporary JSON."""
        self._create_dictionary_synonyms_table()
        self._create_trade_names_table()
        self._create_temp_table()

        self._cursor.execute("DROP TABLE DictionarySynonyms;")
        self._cursor.execute("DROP TABLE TradeNames;")

        self._load_json()
        self._conn.commit()
        self._conn.close()

    def _create_dictionary_synonyms_table(self) -> None:
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

    def _create_trade_names_table(self) -> None:
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

    def _create_temp_table(self) -> None:
        """Create temporary table to store therapies data."""
        create_temp_table = """
            CREATE TEMPORARY TABLE temp(concept_id, label, approval_rating,
                                        src_name, trade_names, aliases);
        """
        self._cursor.execute(create_temp_table)

        insert_temp = f"""
            INSERT INTO temp(concept_id, label, approval_rating, src_name,
                             trade_names, aliases)
            SELECT
                ds.chembl_id,
                ds.pref_name,
                CASE
                    WHEN ds.withdrawn_flag
                        THEN '{ApprovalRating.CHEMBL_WITHDRAWN.value}'
                    WHEN ds.max_phase == 0
                        THEN '{ApprovalRating.CHEMBL_0.value}'
                    WHEN ds.max_phase == 1
                        THEN '{ApprovalRating.CHEMBL_1.value}'
                    WHEN ds.max_phase == 2
                        THEN '{ApprovalRating.CHEMBL_2.value}'
                    WHEN ds.max_phase ==3
                        THEN '{ApprovalRating.CHEMBL_3.value}'
                    WHEN ds.max_phase == 4
                        THEN '{ApprovalRating.CHEMBL_4.value}'
                    ELSE
                        '{None}'
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
                        THEN '{ApprovalRating.CHEMBL_WITHDRAWN.value}'
                    WHEN ds.max_phase == 0
                        THEN '{ApprovalRating.CHEMBL_0.value}'
                    WHEN ds.max_phase == 1
                        THEN '{ApprovalRating.CHEMBL_1.value}'
                    WHEN ds.max_phase == 2
                        THEN '{ApprovalRating.CHEMBL_2.value}'
                    WHEN ds.max_phase ==3
                        THEN '{ApprovalRating.CHEMBL_3.value}'
                    WHEN ds.max_phase == 4
                        THEN '{ApprovalRating.CHEMBL_4.value}'
                    ELSE
                        '{None}'
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

    def _load_json(self) -> None:
        """Load ChEMBL data into database."""
        chembl_data = """
            SELECT
                concept_id,
                label,
                approval_rating,
                src_name,
                trade_names,
                aliases
            FROM temp;
        """
        result = [dict(row) for row in
                  self._cursor.execute(chembl_data).fetchall()]
        self._cursor.execute("DROP TABLE temp;")

        for record in result:
            for attr in ["aliases", "trade_names"]:
                if attr in record and record[attr]:
                    record[attr] = record[attr].split("||")
            self._load_therapy(record)

    def _load_meta(self) -> None:
        """Add ChEMBL metadata."""
        metadata = SourceMeta(data_license="CC BY-SA 3.0",
                              data_license_url="https://creativecommons.org/licenses/by-sa/3.0/",  # noqa: E501
                              version=self._version,
                              data_url=bioversions.resolve("chembl").homepage,
                              rdp_url="http://reusabledata.org/chembl.html",
                              data_license_attributes={
                                  "non_commercial": False,
                                  "share_alike": True,
                                  "attribution": True
                              })
        params = dict(metadata)
        params["src_name"] = SourceName.CHEMBL.value
        self.database.metadata.put_item(Item=params)
