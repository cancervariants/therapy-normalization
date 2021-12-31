"""This module defines the ChEMBL ETL methods."""
import logging
import os
import shutil
import sqlite3
from typing import Optional, List
from pathlib import Path

import chembl_downloader
import bioversions
from disease.query import QueryHandler as DiseaseNormalizer

from therapy.etl.base import Base, DEFAULT_DATA_PATH
from therapy.schemas import SourceName, NamespacePrefix, ApprovalRating, \
    SourceMeta, HasIndication
from therapy.database import Database


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class ChEMBL(Base):
    """Class for ChEMBL ETL methods."""

    def __init__(self, database: Database,
                 data_path: Path = DEFAULT_DATA_PATH):
        """Initialize HemOnc instance.

        :param therapy.database.Database database: application database
        :param Path data_path: path to normalizer data directory
        """
        super().__init__(database, data_path)
        self.disease_normalizer = DiseaseNormalizer(self.database.endpoint_url)

    def _download_data(self) -> None:
        """Download latest ChEMBL database file from EBI."""
        logger.info("Retrieving source data for ChEMBL")
        os.environ["PYSTOW_HOME"] = str(self._src_dir.parent.absolute())
        tmp_path = chembl_downloader.download_extract_sqlite()
        shutil.move(tmp_path, self._src_dir)
        shutil.rmtree(tmp_path.parent.parent.parent)
        logger.info("Successfully retrieved source data for ChEMBL")

    def _extract_data(self, use_existing: bool = False) -> None:
        """Extract data from the ChEMBL source.

        :param bool use_existing: if True, don't try to fetch latest source data
        """
        super()._extract_data(use_existing)
        conn = sqlite3.connect(self._src_file)
        conn.row_factory = sqlite3.Row
        self._conn = conn
        self._cursor = conn.cursor()

    @staticmethod
    def _unwrap_group_concat(value: Optional[str]) -> List[str]:
        """Process concatenated values retrieved from ChEMBL DB.
        :param Optional[str] value: the value provided by the cursor
        :return: List of separated values (empty if none present)
        """
        if value:
            return value.split("||")
        else:
            return []

    @staticmethod
    def _get_approval_rating(value: str) -> ApprovalRating:
        """Standardize approval rating value
        :param str value: value retrieved from ChEMBL database
        :return: instantiated ApprovalRating
        :raise: ValueError if invalid value is provided
        """
        if value == "0":
            return ApprovalRating.CHEMBL_0
        elif value == "1":
            return ApprovalRating.CHEMBL_1
        elif value == "2":
            return ApprovalRating.CHEMBL_2
        elif value == "3":
            return ApprovalRating.CHEMBL_3
        elif value == "4":
            return ApprovalRating.CHEMBL_4
        else:
            raise ValueError(f"Unrecognized approval rating: {value}")

    def _get_indications(self, value: Optional[str]) -> List[HasIndication]:
        """Construct indication objects to prepare for loading into DB
        :param Optional[str] value: `indication` value fetched from ChEMBL DB
        :return: List containing valid HasIndication objects
        """
        if value:
            indications = []
            indication_groups = value.split("|||")
            for group in indication_groups:
                ind_group = group.split("||")
                for term in ind_group[:4]:
                    response = self.disease_normalizer.search_groups(term)
                    if response["match_type"] > 0:
                        descriptor = response["disease_descriptor"]
                        phase = self._get_approval_rating(ind_group[4])
                        indications.append(HasIndication(
                            disease_label=descriptor["label"],
                            normalized_disease_id=descriptor["disease_id"],
                            meta={"chembl_max_phase_for_ind": phase}
                        ))
                        break
            return indications
        else:
            return []

    def _transform_data(self) -> None:
        """Transform SQLite data and load to DB."""
        query = """
        SELECT
            md.chembl_id,
            md.molregno,
            md.pref_name,
            md.max_phase,
            md.withdrawn_flag,
            group_concat(ms.synonyms, '||')   as aliases,
            group_concat(ms_tn.synonyms, '||') as trade_names_syn,
            f.product_id,
            group_concat(p.trade_name, '||')  as trade_names,
            group_concat(d.indication, '|||') as indication
        FROM molecule_dictionary md
        LEFT JOIN (
            SELECT DISTINCT molregno, synonyms, syn_type
            FROM molecule_synonyms
            WHERE syn_type != 'TRADE_NAME'
        ) ms on md.molregno = ms.molregno
        LEFT JOIN(
            SELECT DISTINCT molregno, synonyms, syn_type
            FROM molecule_synonyms
            WHERE syn_type == 'TRADE_NAME'
        ) ms_tn on md.molregno = ms_tn.molregno
        LEFT JOIN formulations f on md.molregno = f.molregno
        LEFT JOIN (
            SELECT DISTINCT product_id, trade_name
            FROM products
        ) p on f.product_id = p.product_id
        LEFT JOIN (
            SELECT DISTINCT
                d.molregno,
                'mesh:' || d.mesh_id || '||' || d.efo_id || '||' || d.mesh_heading
                    || '||' || d.efo_term || '||' || d.max_phase_for_ind as indication
            FROM drug_indication AS d
            WHERE (
                d.molregno IS NOT NULL
                AND d.mesh_heading IS NOT NULL
                AND d.mesh_id IS NOT NULL
                AND d.efo_term IS NOT NULL
                AND d.efo_id IS NOT NULL
                AND d.max_phase_for_ind IS NOT NULL
            )
        ) d on md.molregno = d.molregno
        GROUP BY md.molregno;
        """

        self._cursor.execute(query)
        for row in self._cursor:
            appr_ratings = []
            max_phase = self._get_approval_rating(row["max_phase"])
            if max_phase is not None:
                appr_ratings.append(max_phase)
            if row["withdrawn_flag"] == "1":
                appr_ratings.append(ApprovalRating.CHEMBL_WITHDRAWN)

            has_indication = self._get_indications(row["indication"])

            params = {
                "concept_id": f"{NamespacePrefix.CHEMBL.value}:{row['chembl_id']}",
                "label": row["pref_name"],
                "approval_ratings": appr_ratings,
                "aliases": self._unwrap_group_concat(row["aliases"]),
                "trade_names": self._unwrap_group_concat(
                    row.get("trade_names", []) + row.get("trade_names_syn", [])
                ),
                "has_indication": has_indication
            }
            self._load_therapy(params)
        self._conn.close()

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
