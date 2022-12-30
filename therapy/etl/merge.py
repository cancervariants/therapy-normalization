"""Create concept groups and merged records."""
from typing import Set, Dict, Optional, Any, Tuple
import logging
from timeit import default_timer as timer

from therapy.schemas import SourcePriority
from therapy.database import Database


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class Merge:
    """Handles record merging."""

    def __init__(self, database: Database) -> None:
        """Initialize Merge instance.

        * self._groups is a dictionary keying concept IDs to the Set of concept IDs in
        that group. This is redundantly captured for all group members (i.e., a group
        of 5 concepts would all have their own key, and the value would be an identical
        Set of the same IDs)
        * self._unii_to_drugsatfda keys UNII codes to valid Drugs@FDA concepts.
        Because UNIIs aren't stored directly in groups, we utilize a separate lookup
        table to prevent repeat queries.
        * self._failed_lookups stores concept IDs for which lookups have been attempted
        and failed. Because we don't associate these IDs with groups, a separate
        mapping is necessary to prevent repeat queries.

        :param Database database: db instance to use for record retrieval and creation.
        """
        self.database = database
        self._groups: Dict[str, Set[str]] = {}
        self._unii_to_drugsatfda: Dict[str, Set[str]] = {}
        self._failed_lookups: Set[str] = set()

    def create_merged_concepts(self, record_ids: Set[str]) -> None:
        """Create concept groups, generate merged concept records, and update database.

        :param Set[str] record_ids: concept identifiers from which groups should be
            generated.
        """
        self._create_record_id_sets(record_ids)

        # don't create separate records for single-member groups
        self._groups = {k: v for k, v in self._groups.items() if len(v) > 1}

        logger.info("Creating merged records and updating database...")
        uploaded_ids = set()
        start = timer()
        for record_id, group in self._groups.items():
            if record_id in uploaded_ids:
                continue
            merged_record = self._generate_merged_record(group)

            # add group merger item to DB
            self.database.add_record(merged_record, "merger")

            # add updated references
            for concept_id in group:
                if not self.database.get_record_by_id(concept_id, False):
                    logger.error(f"Updating nonexistent record: {concept_id} "
                                 f"for {merged_record['label_and_type']}")
                else:
                    merge_ref = merged_record["concept_id"].lower()
                    self.database.update_record(concept_id, "merge_ref", merge_ref)
            uploaded_ids |= group
        logger.info("Merged concept generation successful.")
        end = timer()
        logger.debug(f"Generated and added concepts in {end - start} seconds")

    def _rxnorm_brand_lookup(self, record_id: str) -> Optional[str]:
        """Rx Norm provides brand references back to original therapy concepts.  This
        routine checks whether an ID from a concept group requires an additional
        dereference to obtain the original RxNorm record.

        :param str record_id: RxNorm concept ID to check
        :return: concept ID for RxNorm record if successful, None otherwise
        """
        brand_lookup = self.database.get_records_by_type(record_id, "rx_brand")
        n = len(brand_lookup)
        if n == 1:
            lookup_id = brand_lookup[0]["concept_id"]
            db_record = self.database.get_record_by_id(lookup_id,
                                                       False)
            if db_record:
                return db_record["concept_id"]
        elif n > 1:
            logger.warning(f"Brand lookup for {record_id} had {n} matches")
        return None

    def _get_drugsatfda_from_unii(self, ref: Dict) -> Optional[str]:
        """Given an `associated_with` item keying a UNII code to a Drugs@FDA record,
        verify that the record can be safely added to a concept group.
        Drugs@FDA tracks a number of "compound therapies", and provides UNIIs to each
        individual component. If we included them in normalized record sets, they would
        end up merging distinct therapies under the umbrella of the compound group.
        We're excluding Drugs@FDA records with multiple UNIIs as a tentative solution.

        :param Dict ref: `associated_with` item where `label_and_type` includes
            a UNII code and `src_name` == "DrugsAtFDA"
        :return: Drugs@FDA concept ID if record meets rules, None otherwise
        """
        concept_id = ref["concept_id"]
        fetched = self.database.get_record_by_id(concept_id, False)
        if fetched:
            uniis = [a for a
                     in fetched.get("associated_with", [])
                     if a.startswith("unii")]
        else:
            logger.error(f"Couldn't retrieve record for {concept_id} from {ref}")
            return None
        if len(uniis) == 1:
            return fetched["concept_id"]
        else:
            return None

    def _get_xrefs(self, record: Dict[str, Any]) -> Set[str]:
        """Extract references to entries in other sources from a record.

        :param Dict record: record to process
        :return: Set of xref values
        """
        xrefs = set(record.get("xrefs", []))
        unii_xrefs = [a for a in record.get("associated_with", [])
                      if a.startswith("unii:")]
        for unii in unii_xrefs:
            # get Drugs@FDA records that are associated_with this UNII
            drugsatfda_ids = self._unii_to_drugsatfda.get(unii)
            if drugsatfda_ids is not None:
                xrefs |= drugsatfda_ids
                continue

            unii_assoc = self.database.get_records_by_type(unii.lower(),
                                                           "associated_with")
            drugsatfda_refs = set()
            for ref in unii_assoc:
                if ref["src_name"] == "DrugsAtFDA":
                    drugsatfda_ref = self._get_drugsatfda_from_unii(ref)
                    if drugsatfda_ref:
                        drugsatfda_refs.add(drugsatfda_ref)
            self._unii_to_drugsatfda[unii] = drugsatfda_refs
            xrefs |= drugsatfda_refs
        return xrefs

    def _create_record_id_set(
        self, record_id: str, observed_id_set: Optional[Set] = None
    ) -> Set[str]:
        """Create concept ID group for an individual record ID.

        :param str record_id: concept ID for record to build group from
        :return: set of related identifiers pertaining to a common concept.
        """
        if observed_id_set is None:
            observed_id_set = set()

        if record_id in self._groups:
            return self._groups[record_id]
        elif record_id.startswith("drugsatfda"):
            return {record_id}
        elif record_id in self._failed_lookups:
            return observed_id_set - {record_id}

        # get record
        db_record = self.database.get_record_by_id(record_id)
        if not db_record:
            if record_id.startswith("rxcui"):
                brand_lookup = self._rxnorm_brand_lookup(record_id)
                if brand_lookup:
                    return self._create_record_id_set(brand_lookup, observed_id_set)
            logger.warning(f"Unable to resolve lookup for {record_id} in "
                           f"ID set: {observed_id_set}")
            self._failed_lookups.add(record_id)
            return observed_id_set - {record_id}

        # construct group
        local_id_set = self._get_xrefs(db_record)
        if not local_id_set:
            return observed_id_set | {db_record["concept_id"]}
        merged_id_set = {record_id} | observed_id_set
        for local_record_id in local_id_set - observed_id_set:
            merged_id_set |= self._create_record_id_set(local_record_id, merged_id_set)
        return merged_id_set

    def _create_record_id_sets(self, record_ids: Set[str]) -> None:
        """Update self._groups with normalized concept groups.
        :param Set[str] record_ids: concept identifiers from which groups should be
            generated.
        """
        logger.info("Generating record ID sets...")
        start = timer()
        for record_id in record_ids:
            new_group = self._create_record_id_set(record_id)
            if new_group:
                for concept_id in new_group:
                    self._groups[concept_id] = new_group
        end = timer()
        logger.debug(f"Built record ID sets in {end - start} seconds")

    def _generate_merged_record(self, record_id_set: Set[str]) -> Dict:
        """Generate merged record from provided concept ID group.
        Where attributes are 'set-like', they should be combined, and where
        they are 'scalar-like', assign from the highest-priority source where
        that attribute is not None.

        Uses the SourcePriority schema to define source priority.

        :param Set record_id_set: group of concept IDs
        :return: completed merged drug object to be stored in DB
        """
        records = []
        for record_id in record_id_set:
            record = self.database.get_record_by_id(record_id)
            if record:
                records.append(record)
            else:
                logger.error(f"Merge record generator could not retrieve "
                             f"record for {record_id} in {record_id_set}")

        def record_order(record: Dict) -> Tuple[int, str]:
            """Provide priority values of concepts for sort function."""
            src = record["src_name"].upper()
            if src == "DRUGS@FDA":
                src = "DRUGSATFDA"
            if src in SourcePriority.__members__:
                source_rank = SourcePriority[src].value
            else:
                raise Exception(f"Prohibited source: {src} in concept_id "
                                f"{record['concept_id']}")
            return source_rank, record["concept_id"]
        records.sort(key=record_order)

        # initialize merged record
        merged_attrs = {
            "concept_id": records[0]["concept_id"],
            "xrefs": [r["concept_id"] for r in records[1:]],
            "label": None,
            "aliases": set(),
            "trade_names": set(),
            "associated_with": set(),
            "approval_ratings": set(),
            "approval_year": set(),
            "has_indication": [],
        }

        # merge from constituent records
        set_fields = ["aliases", "trade_names", "associated_with", "approval_year"]
        for record in records:
            for field in set_fields:
                merged_attrs[field] |= set(record.get(field, set()))
            approval_ratings = record.get("approval_ratings")
            if approval_ratings:
                merged_attrs["approval_ratings"] |= set(approval_ratings)
            if merged_attrs["label"] is None:
                merged_attrs["label"] = record.get("label")
            for ind in record.get("has_indication", []):
                if ind not in merged_attrs["has_indication"]:
                    merged_attrs["has_indication"].append(ind)

        # clear unused fields
        for field in set_fields + ["has_indication", "approval_ratings"]:
            field_value = merged_attrs[field]
            if field_value:
                merged_attrs[field] = list(field_value)
            else:
                del merged_attrs[field]
        if not merged_attrs["label"]:
            del merged_attrs["label"]

        merged_attrs["label_and_type"] = f"{merged_attrs['concept_id'].lower()}##merger"
        merged_attrs["item_type"] = "merger"
        return merged_attrs
