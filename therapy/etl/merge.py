"""Create concept groups and merged records."""
from therapy.database import Database, RecordNotFoundError
from therapy.schemas import Drug, NamespacePrefix
from typing import Set
import logging


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Merge:
    """Handles record merging."""

    def __init__(self, database: Database):
        """Initialize Merge instance"""
        self.database = database

    def create_record_id_set(self, record_id: str,
                             observed_id_set: Set = set()) -> Set:
        """Create concept ID group

        :param str record_id: concept ID for record to build group from

        :return: group of cross-referencing records
        :rtype: Set
        """
        try:
            record = self.database.get_record_by_id(record_id)
        except RecordNotFoundError:
            logger.error(f"Could not retrieve record for {record_id}"
                         f"ID set: {observed_id_set}")
            return observed_id_set - {record_id}

        local_id_set = set(record.other_identifiers)
        merged_id_set = local_id_set | observed_id_set | {record.concept_id}
        for local_record_id in local_id_set - observed_id_set:
            merged_id_set |= self.create_record_id_set(local_record_id,
                                                       merged_id_set)
        return merged_id_set

    def create_merged_record(self, record_id_set: Set) -> Drug:
        """
        Generate merged record from provided concept ID group.
        Where attributes are sets, they should be merged, and where they are
        scalars, assign from the highest-priority source where that attribute
        is non-null. Priority is NCIt > ChEMBL > DrugBank > Wikidata.

        :param Set record_id_set: group of concept IDs
        :return: merged Drug object
        :rtype: therapy.schemas.Drug
        """
        records = []
        for record_id in record_id_set:
            try:
                records.append(self.database.get_record_by_id(record_id))
            except RecordNotFoundError:
                logger.error(f"Could not retrieve record for {record_id}"
                             f"ID set: {record_id_set}")

        def order(concept_id):
            prefix = concept_id.split(':')[0]
            if prefix == NamespacePrefix.NCIT:
                return 1
            elif prefix == NamespacePrefix.CHEMBL:
                return 2
            elif prefix == NamespacePrefix.DRUGBANK:
                return 3
            elif prefix == NamespacePrefix.WIKIDATA:
                return 4
            else:
                raise Exception(f"Invalid namespace: {concept_id}")

        records.sort(key=order)

        """
        scalars:
            label
            approval status

        sets:
            trade names
            concept ids??? both?
            other ids???
            xrefs

        """
        attrs = {'aliases': set(), 'concept_ids': set(), 'trade_names': set()}
        scalar_fields = ['trade_names', 'xrefs']
        for record in records:
            if 'label' not in attrs and record.label:
                attrs['label'] = record.label
            if 'approval_status' not in attrs and record.approval_status:
                attrs['approval_status'] = record.approval_status
            for s in scalar_fields:  # TODO others? concept id?
                attrs[s] |= set(record[s])

        for s in scalar_fields:
            attrs[s] = list(attrs[s])
