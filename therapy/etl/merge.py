"""Create concept groups and merged records."""
from therapy.database import Database, RecordNotFoundError
from therapy.schemas import Drug, MergedDrug, DynamoDBIdentity, SourceName
from typing import Set
import logging


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Merge:
    """Handles record merging."""

    def __init__(self, database: Database):
        """Initialize Merge instance"""
        self.database = database

    def create_merged_concepts(self, record_ids: Set[str]):
        """Create concept groups and generate merged concept records.

        :param Set[str] record_ids: concept identifiers to create groups of

        TODO
         * Consider moving update method calls into database object
         * Make final call on how to handle dangling IDs
         * When generating merged records, should pull other_id_set if
           merged record already found?
        """
        completed_ids = set()
        for record_id in record_ids:
            if record_id in completed_ids:
                continue
            concept_group = self._create_record_id_set(record_id)
            merged_record = self._generate_merged_record(concept_group)

            self.database.therapies.update_item(
                Key={
                    'label_and_type': f'{record_id.lower()}##identity',
                    'concept_id': record_id
                },
                AttributeUpdates={
                    'merged_record': dict(merged_record)
                }
            )
            for other_record_id in concept_group - {record_id}:
                self.database.therapies.update_item(
                    Key={
                        'label_and_type':
                            f'{other_record_id.lower()}##identity',
                        'concept_id': other_record_id
                    },
                    AttributeUpdates={
                        'merged_record_reference': record_id
                    }
                )
            completed_ids |= concept_group

    def _create_record_id_set(self, record_id: str,
                              observed_id_set: Set = set()) -> Set[str]:
        """Create concept ID group.

        :param str record_id: concept ID for record to build group from
        :return: group of related records pertaining to a common concept.
        :rtype: Set[str]
        """
        try:
            record = Drug(**self.database.get_record_by_id(record_id))
        except RecordNotFoundError:
            # TODO
            # may need to include nonexistent record IDs to enable
            # merging with newly-added records upon future updates
            logger.error(f"Could not retrieve record for {record_id}"
                         f"ID set: {observed_id_set}")
            return observed_id_set | {record_id}

        local_id_set = set(record.other_identifiers)
        merged_id_set = local_id_set | observed_id_set | {record.concept_id}
        for local_record_id in local_id_set - observed_id_set:
            merged_id_set |= self._create_record_id_set(local_record_id,
                                                        merged_id_set)
        return merged_id_set

    def _generate_merged_record(self, record_id_set: Set) -> MergedDrug:
        """Generate merged record from provided concept ID group.
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

        def record_order(record: DynamoDBIdentity):
            src = record.src_name
            if src == SourceName.NCIT:
                return 1
            elif src == SourceName.CHEMBL:
                return 2
            elif src == SourceName.DRUGBANK:
                return 3
            else:
                return 4

        records.sort(key=record_order)

        attrs = {'aliases': set(), 'concept_ids': set(), 'trade_names': set()}
        for record in records:
            values = record.__values__
            for field in ['aliases', 'trade_names']:
                if field in values:
                    attrs[field] |= values[field]
            attrs['concept_ids'].add(values['concept_id'])
            for field in ['label', 'approval_status']:
                if field not in attrs and field in values and values['field']:
                    attrs[field] = values[field]
        for field in ['aliases', 'concept_ids', 'trade_names']:
            attrs[field] = list(attrs[field])

        return MergedDrug(**attrs)
