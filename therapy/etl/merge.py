"""Create concept groups and merged records."""
from therapy.database import Database, RecordNotFoundError
from therapy.schemas import Drug, DBRecord, DBMergedRecord, SourceName, \
    NamespacePrefix
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
         * When updating existing records, how to ensure that no dangling
           records remain after an other_identifier is removed?
        """
        completed_ids = set()
        for record_id in record_ids:
            if record_id in completed_ids:
                continue  # skip if already computed
            concept_group = self._create_record_id_set(record_id)
            merged_record = self._generate_merged_record(concept_group)
            self.database.add_item(merged_record)
            group_id = merged_record.concept_id.split('##')[0]
            concept_group = group_id.split('|')
            for concept_id in concept_group:
                self.database.update_record(concept_id, 'merge_ref', group_id)
            completed_ids |= set(concept_group)

    def _create_record_id_set(self, record_id: str,
                              observed_id_set: Set = set()) -> Set[str]:
        """Create concept ID group.

        :param str record_id: concept ID for record to build group from
        :return: group of related records pertaining to a common concept.
        :rtype: Set[str]
        """
        try:
            db_record = self.database.get_record_by_id(record_id)
            params = dict(db_record)
            if 'other_identifiers' not in params.keys() \
                    or not params['other_identifiers']:
                params['other_identifiers'] = []
            record = Drug(**params)
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

    def _generate_merged_record(self, record_id_set: Set) -> DBMergedRecord:
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

        def record_order(record: DBRecord):
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

        attrs = {'aliases': set(), 'concept_id': set(), 'trade_names': set(),
                 'xrefs': set()}
        for record in records:
            values = record.__values__
            for field in ['aliases', 'trade_names', 'xrefs']:
                if field in values:
                    attrs[field] |= set(values[field])
            attrs['concept_id'].add(values['concept_id'])
            for field in ['label', 'approval_status']:
                if field not in attrs and field in values and values['field']:
                    attrs[field] = values[field]
        for field in ['aliases', 'concept_id', 'trade_names']:
            attrs[field] = list(attrs[field])

        # generate composite concept ID
        def concept_id_order(concept_id: str):
            prefix = concept_id.split(':')
            if prefix == NamespacePrefix.NCIT:
                return 1
            elif prefix == NamespacePrefix.CHEMBL:
                return 2
            elif prefix == NamespacePrefix.DRUGBANK:
                return 3
            else:
                return 4
        attrs['concept_id'] = '|'.join(sorted(attrs['concept_id'],
                                              key=concept_id_order))

        attrs['label_and_type'] = f"{attrs['concept_id']}##merger"
        return DBMergedRecord(**attrs)
