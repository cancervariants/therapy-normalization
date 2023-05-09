"""Provide PostgreSQL client."""
import tarfile
import atexit
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
import tempfile
from datetime import datetime

import psycopg
from psycopg.types.json import Jsonb
from psycopg.errors import DuplicateObject, DuplicateTable, UndefinedTable, \
    UniqueViolation
import requests

from therapy.database import AbstractDatabase, DatabaseException, DatabaseWriteException
from therapy.schemas import RefType, SourceMeta, SourceName, HasIndication


logger = logging.getLogger(__name__)


SCRIPTS_DIR = Path(__file__).parent / "postgresql"


class PostgresDatabase(AbstractDatabase):
    """Database class employing PostgreSQL."""

    def __init__(self, db_url: Optional[str] = None, **db_args) -> None:
        """Initialize Postgres connection.

        :param db_url: libpq compliant database connection URI
        :param **db_args: see below

        :Keyword Arguments:
            * user: Postgres username
            * password: Postgres password (optional or blank if unneeded)
            * db_name: name of database to connect to

        :raise DatabaseInitializationException: if initial setup fails
        """
        if db_url:
            conninfo = db_url
        elif "THERAPY_NORM_DB_URL" in os.environ:
            conninfo = os.environ["THERAPY_NORM_DB_URL"]
        else:
            user = db_args.get("user", "postgres")
            password = db_args.get("password", "")
            db_name = db_args.get("db_name", "therapy_normalizer")
            if password:
                conninfo = f"dbname={db_name} user={user} password={password}"
            else:
                conninfo = f"dbname={db_name} user={user}"

        self.conn = psycopg.connect(conninfo)
        self.initialize_db()
        self._cached_sources = {}

        atexit.register(self.close_connection)

    _list_tables_query = b"""SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';
    """

    def list_tables(self) -> List[str]:
        """Return names of tables in database.

        :return: Table names in database
        """
        with self.conn.cursor() as cur:
            cur.execute(self._list_tables_query)
            tables = cur.fetchall()
        return [t[0] for t in tables]

    _drop_query = b"""
    DROP MATERIALIZED VIEW IF EXISTS record_lookup_view;
    DROP TABLE IF EXISTS
        therapy_aliases,
        therapy_associations,
        therapy_concepts,
        therapy_labels,
        therapy_merged,
        therapy_sources,
        therapy_trade_names,
        therapy_rx_brand_ids,
        therapy_xrefs;
    """

    def drop_db(self) -> None:
        """Perform complete teardown of DB. Useful for quickly resetting all data or
        reconstructing after apparent schema error. If in a protected environment,
        require confirmation.

        :raise DatabaseWriteException: if called in a protected setting with
            confirmation silenced.
        """
        try:
            if not self._check_delete_okay():
                return
        except DatabaseWriteException as e:
            raise e

        with self.conn.cursor() as cur:
            cur.execute(self._drop_query)
            self.conn.commit()
        logger.info("Dropped all existing therapy normalizer tables.")

    def check_schema_initialized(self) -> bool:
        """Check if database schema is properly initialized.

        :return: True if DB appears to be fully initialized, False otherwise
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute((SCRIPTS_DIR / "create_tables.sql").read_bytes())
        except DuplicateTable:
            self.conn.rollback()
        else:
            logger.info("Therapy table existence check failed.")
            self.conn.rollback()
            return False
        try:
            with self.conn.cursor() as cur:
                cur.execute((SCRIPTS_DIR / "add_fkeys.sql").read_bytes())
        except DuplicateObject:
            self.conn.rollback()
        else:
            logger.info("Therapy foreign key existence check failed.")
            self.conn.rollback()
            return False
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    (SCRIPTS_DIR / "create_record_lookup_view.sql").read_bytes()
                )
        except DuplicateTable:
            self.conn.rollback()
        else:
            logger.info("Therapy normalized view lookup failed.")
            self.conn.rollback()
            return False
        try:
            with self.conn.cursor() as cur:
                cur.execute((SCRIPTS_DIR / "add_indexes.sql").read_bytes())
        except DuplicateTable:
            self.conn.rollback()
        else:
            logger.info("Therapy indexes check failed.")
            self.conn.rollback()
            return False

        return True

    _check_sources_query = b"SELECT name FROM therapy_sources;"
    _check_concepts_query = b"SELECT COUNT(1) FROM therapy_concepts LIMIT 1;"
    _check_merged_query = b"SELECT COUNT(1) FROM therapy_merged LIMIT 1;"

    def check_tables_populated(self) -> bool:
        """Perform rudimentary checks to see if tables are populated.

        Emphasis is on rudimentary -- if some demonic element has deleted half of the
        therapy aliases, this method won't pick it up. It just wants to see if a few
        critical tables have at least a small number of records.

        :return: True if queries successful, false if DB appears empty
        """
        with self.conn.cursor() as cur:
            cur.execute(self._check_sources_query)
            results = cur.fetchall()
        if len(results) < len(SourceName):
            logger.info("Therapy sources table is missing expected sources.")
            return False

        with self.conn.cursor() as cur:
            cur.execute(self._check_concepts_query)
            result = cur.fetchone()
        if not result or result[0] < 1:
            logger.info("Therapy records table is empty.")
            return False

        with self.conn.cursor() as cur:
            cur.execute(self._check_merged_query)
            result = cur.fetchone()
        if not result or result[0] < 1:
            logger.info("Normalized therapy records table is empty.")
            return False

        return True

    def initialize_db(self) -> None:
        """Check if DB is set up. If not, create tables/indexes/views."""
        if not self.check_schema_initialized():
            self.drop_db()
            self._create_tables()
            self._create_views()
            self._add_indexes()

    def _create_views(self) -> None:
        """Create materialized views."""
        create_view_query = (SCRIPTS_DIR / "create_record_lookup_view.sql").read_bytes()
        with self.conn.cursor() as cur:
            cur.execute(create_view_query)
            self.conn.commit()

    _refresh_views_query = b"REFRESH MATERIALIZED VIEW record_lookup_view;"

    def _refresh_views(self) -> None:
        """Update materialized views.

        Not responsible for ensuring existence of views. Calling functions should
        either check beforehand or catch psycopg.UndefinedTable.
        """
        with self.conn.cursor() as cur:
            cur.execute(self._refresh_views_query)
            self.conn.commit()

    def _add_fkeys(self) -> None:
        """Add fkey relationships."""
        add_fkey_query = (SCRIPTS_DIR / "add_fkeys.sql").read_bytes()
        with self.conn.cursor() as cur:
            cur.execute(add_fkey_query)
            self.conn.commit()

    def _drop_fkeys(self) -> None:
        """Drop fkey relationships."""
        drop_fkey_query = (SCRIPTS_DIR / "drop_fkeys.sql").read_bytes()
        with self.conn.cursor() as cur:
            cur.execute(drop_fkey_query)
            self.conn.commit()

    def _add_indexes(self) -> None:
        """Create core search indexes."""
        add_indexes_query = (SCRIPTS_DIR / "add_indexes.sql").read_bytes()
        with self.conn.cursor() as cur:
            cur.execute(add_indexes_query)
            self.conn.commit()

    def _drop_indexes(self) -> None:
        """Drop all custom indexes."""
        drop_indexes_query = (SCRIPTS_DIR / "drop_indexes.sql").read_bytes()
        with self.conn.cursor() as cur:
            cur.execute(drop_indexes_query)
            self.conn.commit()

    def _create_tables(self) -> None:
        """Create all tables, indexes, and views."""
        logger.debug("Creating new therapy normalizer tables.")
        tables_query = (SCRIPTS_DIR / "create_tables.sql").read_bytes()

        with self.conn.cursor() as cur:
            cur.execute(tables_query)
            self.conn.commit()

    _get_metadata_query = b"SELECT * FROM therapy_sources WHERE name = %s;"

    def get_source_metadata(self, src_name: Union[str, SourceName]) -> Optional[Dict]:
        """Get license, versioning, data lookup, etc information for a source.

        :param src_name: name of the source to get data for
        :return: Dict containing metadata if lookup is successful
        """
        if isinstance(src_name, SourceName):
            src_name = src_name.value

        if src_name in self._cached_sources:
            return self._cached_sources[src_name]

        with self.conn.cursor() as cur:
            cur.execute(self._get_metadata_query, [src_name])
            metadata_result = cur.fetchone()
            if not metadata_result:
                return None
            metadata = {
                "data_license": metadata_result[1],
                "data_license_url": metadata_result[2],
                "version": metadata_result[3],
                "data_url": metadata_result[4],
                "rdp_url": metadata_result[5],
                "data_license_attributes": {
                    "non_commercial": metadata_result[6],
                    "attribution": metadata_result[7],
                    "share_alike": metadata_result[8],
                }
            }
            self._cached_sources[src_name] = metadata
            return metadata

    _get_record_query = b"SELECT * FROM record_lookup_view WHERE lower(concept_id) = %s;"  # noqa: E501

    def _get_record(self, concept_id: str, case_sensitive: bool) -> Optional[Dict]:
        """Retrieve non-merged record. The query is pretty different, so this method
        is broken out for PostgreSQL.

        :param concept_id: ID of concept to get
        :param case_sensitive: record lookups are performed using a case-insensitive
            index, so this parameter isn't used by Postgres
        :return: complete record object if successful
        """
        concept_id_param = concept_id.lower()

        with self.conn.cursor() as cur:
            cur.execute(self._get_record_query, [concept_id_param])
            result = cur.fetchone()
        if not result:
            return None

        therapy_record = {
            "concept_id": result[0],
            "label": result[1],
            "aliases": result[2],
            "associated_with": result[3],
            "trade_names": result[4],
            "xrefs": result[5],
            "has_indication": result[6],  # TODO ???
            "approval_ratings": result[7],  # TODO ???
            "approval_year": result[8],  # TODO ???
            "src_name": result[9],
            "merge_ref": result[10],
            "item_type": "identity",
        }
        if therapy_record.get("has_indication"):
            ind = therapy_record["has_indication"]
            therapy_record["has_indication"] = [
                HasIndication(
                    disease_id=ind[0],
                    disease_label=ind[1],
                    normalized_disease_id=ind[2],
                    supplemental_info=ind[3]
                )
                for ind in ind
            ]
        return {k: v for k, v in therapy_record.items() if v}

    _get_merged_record_query = b"SELECT * FROM therapy_merged WHERE lower(concept_id) = %s;"  # noqa: E501

    def _get_merged_record(
        self, concept_id: str, case_sensitive: bool
    ) -> Optional[Dict]:
        """Retrieve normalized record from DB.

        :param concept_id: normalized ID for the merged record
        :param case_sensitive: record lookups are performed using a case-insensitive
            index, so this parameter isn't used by Postgres
        :return: normalized record if successful
        """
        concept_id = concept_id.lower()
        with self.conn.cursor() as cur:
            cur.execute(self._get_merged_record_query, [concept_id])
            result = cur.fetchone()
        if not result:
            return None

        merged_record = {
            "concept_id": result[0],
            "label": result[1],
            "aliases": result[2],
            "associated_with": result[3],
            "trade_names": result[4],
            "xrefs": result[5],
            "approval_ratings": result[6],  # TODO ??
            "approval_year": result[7],  # TODO ???
            "has_indication": result[8],  # TODO ???
            "item_type": "merger",
        }
        return {k: v for k, v in merged_record.items() if v}

    def get_record_by_id(self, concept_id: str, case_sensitive: bool = True,
                         merge: bool = False) -> Optional[Dict]:
        """Fetch record corresponding to provided concept ID

        :param str concept_id: concept ID for therapy record
        :param bool case_sensitive: not used by Postgresql implementation
        :param bool merge: if true, look for merged record; look for identity record
        otherwise.
        :return: complete therapy record, if match is found; None otherwise
        """
        if merge:
            return self._get_merged_record(concept_id, case_sensitive)
        else:
            return self._get_record(concept_id, case_sensitive)

    _ref_types_query = {
        RefType.LABEL: b"SELECT concept_id FROM therapy_labels WHERE lower(label) = %s;",  # noqa: E501
        RefType.TRADE_NAMES: b"SELECT concept_id FROM therapy_trade_names WHERE lower(trade_name) = %s;",  # noqa: E501
        RefType.ALIASES: b"SELECT concept_id FROM therapy_aliases WHERE lower(alias) = %s;",  # noqa: E501
        RefType.XREFS: b"SELECT concept_id FROM therapy_xrefs WHERE lower(xref) = %s;",
        RefType.ASSOCIATED_WITH: b"SELECT concept_id FROM therapy_associations WHERE lower(associated_with) = %s;"  # noqa: E501
    }

    def get_refs_by_type(self, search_term: str, ref_type: RefType) -> List[str]:
        """Retrieve concept IDs for records matching the user's query. Other methods
        are responsible for actually retrieving full records.

        :param search_term: string to match against
        :param ref_type: type of match to look for.
        :return: list of associated concept IDs. Empty if lookup fails.
        """
        # TODO handle rx brand here?
        query = self._ref_types_query.get(ref_type)
        if not query:
            raise ValueError("invalid reference type")

        with self.conn.cursor() as cur:
            cur.execute(query, (search_term.lower(), ))
            concept_ids = cur.fetchall()
        if concept_ids:
            return [i[0] for i in concept_ids]
        else:
            return []

    _get_rxnorm_id_by_brand_query = b"SELECT concept_id FROM therapy_rx_brand_ids WHERE lower(rxcui) = %s;"  # noqa: E501

    def get_rxnorm_id_by_brand(self, brand_id: str) -> Optional[str]:
        """Given RxNorm brand ID, retrieve associated drug concept ID.

        :param brand_id: rxcui brand identifier to dereference
        :return: RxNorm therapy concept ID if successful, None otherwise
        """
        with self.conn.cursor() as cur:
            cur.execute(self._get_rxnorm_id_by_brand_query, (brand_id, ))
            results = cur.fetchall()
        if results and len(results) == 1:
            return results[0][0]
        else:
            return None

    # TODO: rewrite as materialized view
    _get_dafda_unii_query = b"""
    SELECT concept_id FROM (
        SELECT concept_id, (SELECT unnest(array_agg(associated_with))) as unii
        FROM therapy_associations ta
        WHERE associated_with ILIKE 'unii:%' AND concept_id ILIKE 'drugsatfda.%'
        GROUP BY concept_id
        HAVING count(associated_with) = 1
    ) valid_dafda_uniis
    WHERE unii = %s;
    """

    def get_drugsatfda_from_unii(self, unii: str) -> Set[str]:
        """Get Drugs@FDA IDs associated with a single UNII, given that UNII. Used
        in merged concept generation.

        :param unii: UNII to find associations for
        :return: set of directly associated Drugs@FDA concept IDs.
        """
        with self.conn.cursor() as cur:
            cur.execute(self._get_dafda_unii_query, (unii, ))
            results = cur.fetchall()
        return {d[0] for d in results}
        # dafda_concepts = set()
        # associated_concepts = self.get_refs_by_type(unii, RefType.ASSOCIATED_WITH)
        # for concept_id in associated_concepts:
        #     if concept_id.startswith("drugsatfda"):
        #         record = self.get_record_by_id(concept_id)
        #         if not record:
        #             continue
        #         uniis = [
        #             a for a in record.get("associated_with", [])
        #             if a.startswith("unii")
        #         ]
        #         if len(uniis) == 1:
        #             dafda_concepts.add(concept_id)
        # return dafda_concepts

    _get_source_concept_ids_query = b"""
        SELECT concept_id FROM therapy_concepts tc
        LEFT JOIN therapy_sources ts ON ds.name = tc.source
        WHERE ts.name = %s;
    """
    _get_all_ids_query = b"SELECT concept_id FROM therapy_concepts;"

    def get_all_concept_ids(self, source: Optional[SourceName] = None) -> Set[str]:
        """Retrieve concept IDs for use in generating normalized records.

        :param source: optionally, just get all IDs for a specific source
        :return: Set of concept IDs as strings.
        """
        if source is not None:
            query = self._get_source_concept_ids_query
        else:
            query = self._get_all_ids_query

        with self.conn.cursor() as cur:
            if source is None:
                cur.execute(query)
            else:
                cur.execute(query, (source, ))
            ids_tuple = cur.fetchall()
        return {i[0] for i in ids_tuple}

    _add_source_metadata_query = b"""
    INSERT INTO therapy_sources(
        name, data_license, data_license_url, version, data_url, rdp_url,
        data_license_nc, data_license_attr, data_license_sa
    )
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s );
    """

    def add_source_metadata(self, src_name: SourceName, meta: SourceMeta) -> None:
        """Add new source metadata entry.

        :param src_name: name of source
        :param meta: known source attributes
        :raise DatabaseWriteException: if write fails
        """
        with self.conn.cursor() as cur:
            cur.execute(
                self._add_source_metadata_query,
                [
                    src_name.value,
                    meta.data_license, meta.data_license_url, meta.version,
                    meta.data_url, meta.rdp_url,
                    meta.data_license_attributes["non_commercial"],
                    meta.data_license_attributes["attribution"],
                    meta.data_license_attributes["share_alike"],
                ]
            )
        self.conn.commit()

    _add_rxnorm_brand_query = b"INSERT INTO therapy_rx_brand_ids (rxcui, concept_id) VALUES (%s, %s);"  # noqa: E501

    def add_rxnorm_brand(self, brand_id: str, record_id: str) -> None:
        """Add RxNorm brand association to an existing RxNorm concept.

        :param brand_id: ID of RxNorm brand concept
        :param record_id: ID of RxNorm drug concept
        """
        with self.conn.cursor() as cur:
            cur.execute(self._add_rxnorm_brand_query, (brand_id, record_id))
        self.conn.commit()

    _add_record_query = b"""
        INSERT INTO therapy_concepts (
            concept_id, source, approval_ratings, approval_year, has_indication
        )
        VALUES (%s, %s, %s, %s, %s);
    """
    _insert_label_query = b"INSERT INTO therapy_labels (label, concept_id) VALUES (%s, %s);"  # noqa: E501
    _insert_alias_query = b"INSERT INTO therapy_aliases (alias, concept_id) VALUES (%s, %s);"  # noqa: E501
    _insert_trade_name_query = b"INSERT INTO therapy_trade_names (trade_name, concept_id) VALUES (%s, %s);"  # noqa: E501
    _insert_xref_query = b"INSERT INTO therapy_xrefs (xref, concept_id) VALUES (%s, %s);"  # noqa: E501
    _insert_assoc_query = b"INSERT INTO therapy_associations (associated_with, concept_id) VALUES (%s, %s);"  # noqa: E501

    @staticmethod
    def _jsonify_indications(indication: Optional[List]) -> Optional[List]:
        """TODO"""
        if indication is None:
            return None
        return [Jsonb(s) for s in indication]

    def add_record(self, record: Dict, src_name: SourceName) -> None:
        """Add new record to database.

        :param record: record to upload
        :param src_name: name of source for record. Not used by PostgreSQL instance.
        """
        concept_id = record["concept_id"]
        indications_entry = self._jsonify_indications(record.get("has_indication"))
        with self.conn.cursor() as cur:
            try:
                cur.execute(self._add_record_query, [
                    concept_id,
                    src_name.value,
                    record.get("approval_ratings"),
                    record.get("approval_year"),
                    indications_entry  # TODO wip
                ])
                if record.get("label"):
                    cur.execute(self._insert_label_query, [record["label"], concept_id])
                for a in record.get("aliases", []):
                    cur.execute(self._insert_alias_query, [a, concept_id])
                for tn in record.get("trade_names", []):
                    cur.execute(self._insert_trade_name_query, [tn, concept_id])
                for x in record.get("xrefs", []):
                    cur.execute(self._insert_xref_query, [x, concept_id])
                for a in record.get("associated_with", []):
                    cur.execute(self._insert_assoc_query, [a, concept_id])
                self.conn.commit()
            except UniqueViolation:
                logger.error(f"Record with ID {concept_id} already exists")
                self.conn.rollback()

    _add_merged_record_query = b"""
    INSERT INTO therapy_merged (
        concept_id, label, aliases, associated_with, trade_names, xrefs,
        approval_ratings, approval_year, has_indication
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    def add_merged_record(self, record: Dict) -> None:
        """Add merged record to database.

        :param record: merged record to add
        """
        with self.conn.cursor() as cur:
            cur.execute(self._add_merged_record_query, [
                record["concept_id"],
                record["label"],
                record.get("aliases"),
                record.get("associated_with"),
                record.get("trade_names"),
                record.get("xrefs"),
                record.get("approval_ratings"),
                record.get("approval_year"),
                record.get("has_indication")
            ])
            self.conn.commit()

    _update_merge_ref_query = b"""
        UPDATE therapy_concepts
        SET merge_ref = %(merge_ref)s
        WHERE concept_id = %(concept_id)s;
    """

    def update_merge_ref(self, concept_id: str, merge_ref: str) -> None:
        """Update the merged record reference of an individual record to a new value.

        :param concept_id: record to update
        :param merge_ref: new ref value
        :raise DatabaseWriteException: if attempting to update non-existent record
        """
        with self.conn.cursor() as cur:
            cur.execute(
                self._update_merge_ref_query,
                {"merge_ref": merge_ref, "concept_id": concept_id}
            )
            row_count = cur.rowcount
            self.conn.commit()

        # UPDATE will fail silently unless we check the # of affected rows
        if row_count < 1:
            raise DatabaseWriteException(
                f"No such record exists for primary key {concept_id}"
            )

    def delete_normalized_concepts(self) -> None:
        """Remove merged records from the database. Use when performing a new update
        of normalized data.

        It would be faster to drop the entire table and do a cascading delete onto the
        merge_ref column in therapy_concepts, but that requires an exclusive access lock
        on the DB, which can be annoying (ie you couldn't have multiple processes
        accessing it, or PgAdmin, etc...)

        :raise DatabaseReadException: if DB client requires separate read calls and
            encounters a failure in the process
        :raise DatabaseWriteException: if deletion call fails
        """
        with self.conn.cursor() as cur:
            cur.execute((SCRIPTS_DIR / "delete_normalized_concepts.sql").read_bytes())
            self.conn.commit()
        self._create_tables()

    _drop_aliases_query = b"""
    DELETE FROM therapy_aliases WHERE id IN (
        SELECT ta.id FROM therapy_aliases ta LEFT JOIN therapy_concepts tc
            ON tc.concept_id = ta.concept_id
        WHERE tc.source = %s
    );
    """
    _drop_associations_query = b"""
    DELETE FROM therapy_associations WHERE id IN (
        SELECT ta.id FROM therapy_associations ta LEFT JOIN therapy_concepts tc
            ON tc.concept_id = ta.concept_id
        WHERE tc.source = %s
    );
    """
    _drop_labels_query = b"""
    DELETE FROM therapy_labels WHERE id IN (
        SELECT ts.id FROM therapy_labels ts LEFT JOIN therapy_concepts tc
            ON tc.concept_id = ts.concept_id
        WHERE tc.source = %s
    );
    """
    _drop_trade_names_query = b"""
    DELETE FROM therapy_trade_names WHERE id IN (
        SELECT ttn.id FROM therapy_trade_names ttn LEFT JOIN therapy_concepts tc
            ON tc.concept_id = ttn.concept_id
        WHERE tc.source = %s
    );
    """
    _drop_xrefs_query = b"""
    DELETE FROM therapy_xrefs WHERE id IN (
        SELECT tx.id FROM therapy_xrefs tx LEFT JOIN therapy_concepts tc
            ON tc.concept_id = tx.concept_id
        WHERE tc.source = %s
    );
    """
    _drop_rxbrands_query = b"""
    DELETE FROM therapy_rx_brand_ids;
    """
    _drop_concepts_query = b"DELETE FROM therapy_concepts WHERE source = %s;"
    _drop_source_query = b"DELETE FROM therapy_sources ts WHERE ts.name = %s;"

    def delete_source(self, src_name: SourceName) -> None:
        """Delete all data for a source. Use when updating source data.

        All of the foreign key relations make deletes *extremely* slow, so this method
        drops and then re-adds them once deletes are finished. This makes it a little
        brittle, and it'd be nice to revisit in the future to perform as a single
        atomic transaction.

        Refreshing the materialized view at the end might be redundant, because
        this method will almost always be called right before more data is written,
        but it's probably necessary just in case that doesn't happen.

        :param src_name: name of source to delete
        :raise DatabaseWriteException: if deletion call fails
        """
        with self.conn.cursor() as cur:
            cur.execute(self._drop_aliases_query, [src_name.value])
            cur.execute(self._drop_associations_query, [src_name.value])
            cur.execute(self._drop_labels_query, [src_name.value])
            cur.execute(self._drop_trade_names_query, [src_name.value])
            cur.execute(self._drop_xrefs_query, [src_name.value])
            if src_name == SourceName.RXNORM:
                cur.execute(self._drop_rxbrands_query)
        self._drop_fkeys()
        self._drop_indexes()

        with self.conn.cursor() as cur:
            cur.execute(self._drop_concepts_query, [src_name.value])
            cur.execute(self._drop_source_query, [src_name.value])
            self.conn.commit()

        self._add_fkeys()
        self._add_indexes()
        self._refresh_views()

    def complete_write_transaction(self) -> None:
        """Conclude transaction or batch writing if relevant."""
        if not self.conn.closed:
            try:
                self._refresh_views()
            except UndefinedTable:
                self.conn.rollback()

    def close_connection(self) -> None:
        """Perform any manual connection closure procedures if necessary."""
        if not self.conn.closed:
            self.conn.commit()
            self.conn.close()

    def load_from_remote(self, url: Optional[str]) -> None:
        """Load DB from remote dump. Warning: Deletes all existing data. If not
        passed as an argument, will try to grab latest release from VICC S3 bucket.

        :param url: location of .tar.gz file created from output of pg_dump
        :raise DatabaseException: if unable to retrieve file from URL or if psql
            command fails
        """
        if not url:
            url = "https://vicc-normalizers.s3.us-east-2.amazonaws.com/therapy_normalization/postgresql/therapy_norm_latest.sql.tar.gz"  # noqa: E501
        with tempfile.TemporaryDirectory() as tempdir:
            tempdir_path = Path(tempdir)
            temp_tarfile = tempdir_path / "therapy_norm_latest.tar.gz"
            with requests.get(url, stream=True) as r:
                try:
                    r.raise_for_status()
                except requests.HTTPError:
                    raise DatabaseException(
                        f"Unable to retrieve PostgreSQL dump file from {url}"
                    )
                with open(temp_tarfile, "wb") as h:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            h.write(chunk)
            tar = tarfile.open(temp_tarfile, "r:gz")
            tar_dump_file = [
                f for f in tar.getmembers() if f.name.startswith("therapy_norm_")
            ][0]
            tar.extractall(path=tempdir_path, members=[tar_dump_file])
            dump_file = tempdir_path / tar_dump_file.name

            if self.conn.info.password:
                pw_param = f"-W {self.conn.info.password}"
            else:
                pw_param = "-w"

            self.drop_db()
            system_call = f"psql -d {self.conn.info.dbname} -U {self.conn.info.user} {pw_param} -f {dump_file.absolute()}"  # noqa: E501
            result = os.system(system_call)
        if result != 0:
            raise DatabaseException(
                f"System call '{result}' returned failing exit code."
            )

    def export_db(self, output_directory: Path) -> None:
        """Dump DB to specified location.

        :param export_location: path to directory to save DB dump in
        :return: Nothing, but saves results of pg_dump to file named
            `therapy_norm_<date and time>.sql`
        :raise ValueError: if output directory isn't a directory or doesn't exist
        :raise DatabaseException: if psql call fails
        """
        if not output_directory.is_dir() or not output_directory.exists():
            raise ValueError(f"Output location {output_directory} isn't a directory or doesn't exist")  # noqa: E501
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        output_location = output_directory / f"therapy_norm_{now}.sql"
        user = self.conn.info.user
        host = self.conn.info.host
        port = self.conn.info.port
        database_name = self.conn.info.dbname
        if self.conn.info.password:
            pw_param = f"-W {self.conn.info.password}"
        else:
            pw_param = "-w"

        system_call = f"pg_dump -E UTF8 -f {output_location} -U {user} {pw_param} -h {host} -p {port} {database_name}"  # noqa: E501
        result = os.system(system_call)
        if result != 0:
            raise DatabaseException(
                f"System call '{system_call}' returned failing exit code."
            )
