"""Create concept groups and merged records."""
from typing import Set, Dict, Tuple
import logging
from timeit import default_timer as timer

from therapy.schemas import SourcePriority
from therapy.database import Database


logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class NormalizeBuilder:
    """Handles record merging."""

    def __init__(self, database: Database) -> None:
        """Initialize NormalizeBuilder instance.

        * self._groups is a dictionary keying concept IDs to the Set of concept IDs in
        that group. This is redundantly captured for all group members (i.e., a group
        of 5 concepts would all have their own key, and the value would be an identical
        Set of the same IDs)
        * self._rxnorm_brand_refs keys RxNorm drug brand concept IDs to corresponding
        RxNorm drug concepts. This is used to associate references to those brands
        to the drugs themselves.
        * self._daf_unii_refs keys Drugs@FDA concepts to UNII codes. This enables
        association between Drugs@FDA records and concepts that reference those UNIIs.

        :param Database database: db instance to use for record retrieval and creation.
        """
        self.database = database
        self._groups: Dict[str, Set[str]] = {}
        self._rxnorm_brand_refs: Dict[str, str] = {}
        self._daf_unii_refs: Dict[str, str] = {}

    def _update_groups_from_record(self, record: Dict) -> None:
        """Update concept groups given next record
        :param Dict record: object as stored in database
        """
        try:
            item_type = record["item_type"]
        except KeyError:
            print(record)
            return
        if item_type == "identity":
            concept_group = {record["concept_id"]} | set(record.get("xrefs", set()))
            concept_group |= {ref for ref in record.get("associated_with", [])
                              if ref.startswith("unii:")}
            new_concept_group = set()
            for concept_id in concept_group:
                if concept_id.lower() in self._groups:
                    new_concept_group |= self._groups[concept_id.lower()]
            concept_group |= new_concept_group
            for concept_id in concept_group:
                self._groups[concept_id.lower()] = concept_group

            if record["src_name"] == "DrugsAtFDA":
                unii_refs = [ref for ref in record.get("xrefs", [])
                             if ref.startswith("unii:")]
                if len(unii_refs) == 1:
                    self._daf_unii_refs[record["concept_id"].lower()] = unii_refs[0]
        elif item_type == "rx_brand":
            ref = record["label_and_type"].split("##")[0]
            self._rxnorm_brand_refs[ref.lower()] = record["concept_id"]

    def _apply_refs(self, ref_map: Dict[str, str]) -> None:
        """Given reference map, update concept group dictionary. Used to process RxNorm
        brand refs and Drugs@FDA UNII refs.
        :param Dict[str, str] ref_map: map keying inbound references to outbound
        references. For RxNorm, this is RxNorm brand ID -> RxNorm concept ID. For
        Drugs@FDA, this is Drugs@FDA application ID -> UNII code.
        """
        for ref, concept in ref_map.items():
            ref_group = self._groups.get(ref.lower())
            concept_group = self._groups.get(concept.lower())
            if ref_group and concept_group:
                new_group = ref_group | concept_group
                for concept_id in new_group:
                    self._groups[concept_id.lower()] = new_group

    def _create_normalized_groups(self) -> None:
        """Generate normalized concept groupings."""
        for record in self.database.scan():
            self._update_groups_from_record(record)

        self._apply_refs(self._rxnorm_brand_refs)
        self._apply_refs(self._daf_unii_refs)

    def create_merged_concepts(self) -> None:
        """Create concept groups, generate merged concept records, and update database.

        :param Set[str] record_ids: concept identifiers from which groups should be
            generated.
        """
        logger.info("Generating record ID sets...")
        start = timer()
        self._create_normalized_groups()
        end = timer()
        logger.debug(f"Built record ID sets in {end - start} seconds")

        # don't create separate records for single-member groups or for UNII codes
        self._groups = {k: v for k, v in self._groups.items()
                        if len(v) > 1 and not k.startswith("unii:")}

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
