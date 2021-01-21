"""Create concept groups and merged records."""
from therapy.schemas import SourceName, NamespacePrefix
from therapy.database import Database
from typing import Set, Dict
import logging

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)

DISALLOWED_SOURCES = {NamespacePrefix.DRUGBANK.value,
                      NamespacePrefix.CHEMBL.value}


class Merge:
    """Handles record merging."""

    def __init__(self, database: Database):
        """Initialize Merge instance.

        :param Database database: db instance to use for record retrieval
            and creation.
        """
        self._database = database
        self._groups = {}  # dict keying concept IDs to group Sets

    def create_merged_concepts(self, record_ids: Set[str]):
        """Create concept groups, generate merged concept records, and
        update database.

        :param Set[str] record_ids: concept identifiers to create groups of

        TODO
         * Consider moving update method calls into database object
         * Make final call on how to handle dangling IDs
         * When generating merged records, should poll other_id_set if
           merged record already found?
         * When updating existing records, how to ensure that no dangling
           records remain after an other_identifier is removed?
        """
        for record_id in record_ids:
            new_group = self._create_record_id_set(record_id)
            for concept_id in new_group:
                self._groups[concept_id] = new_group

        uploaded_ids = set()
        for record_id, group in self._groups.items():
            if record_id in uploaded_ids:
                continue
            merged_record = self._generate_merged_record(group)  # noqa

            # add group merger item to DB
            for concept_id in group:
                self._database.update_record(concept_id, 'merge_ref',
                                             merged_record['label_and_type'])
            uploaded_ids |= group

    def _get_other_ids(self, record: Dict) -> Set[str]:
        """Extract references to entries in other sources from a record.
        Crucially, filter to allowed sources only.

        :param Dict record: record to process
        :return: Set of other_identifier values
        :rtype: Set
        """
        other_ids = set()
        for other_id in record['other_identifiers']:
            allowed = True
            for prefix in DISALLOWED_SOURCES:
                if other_id.startswith(prefix):
                    allowed = False
                    continue
            if allowed:
                other_ids.add(other_id)
        return other_ids

    def _create_record_id_set(self, record_id: str,
                              observed_id_set: Set = set()) -> Set[str]:
        """Create concept ID group for an individual record ID.

        :param str record_id: concept ID for record to build group from
        :return: set of related identifiers pertaining to a common concept.
        """
        if record_id in self._groups:
            return self._groups[record_id]
        else:
            db_record = self._database.get_record_by_id(record_id)

            if not db_record:
                logger.error(f"Could not retrieve record for {record_id} "
                             f"in ID set: {observed_id_set}")
                return observed_id_set | {record_id}
            elif 'other_identifiers' not in db_record:
                return observed_id_set | {record_id}

            local_id_set = self._get_other_ids(db_record)
            merged_id_set = local_id_set | observed_id_set | \
                {record_id}

            for local_record_id in local_id_set - observed_id_set:
                merged_id_set |= self._create_record_id_set(local_record_id,
                                                            merged_id_set)
            return merged_id_set

    def _generate_merged_record(self, record_id_set: Set) -> Dict:
        """Generate merged record from provided concept ID group.
        Where attributes are sets, they should be merged, and where they are
        scalars, assign from the highest-priority source where that attribute
        is non-null.

        Priority is RxNorm > NCIt > ChemIDplus > Wikidata.


        :param Set record_id_set: group of concept IDs
        :return: completed merged drug object to be stored in DB
        """
        records = []
        for record_id in record_id_set:
            record = self._database.get_record_by_id(record_id)
            if record:
                records.append(record)
            else:
                logger.error(f"Could not retrieve record for {record_id}"
                             f"in {record_id_set}")

        def record_order(record):
            """Provide priority values of concepts for sort function."""
            src = record['src_name']
            if src == SourceName.RXNORM:
                source_rank = 1
            elif src == SourceName.NCIT:
                source_rank = 2
            elif src == SourceName.CHEMIDPLUS:
                source_rank = 5
            elif src == SourceName.WIKIDATA:
                source_rank = 6
            else:
                raise Exception(f"Prohibited source: {src} in concept_id "
                                f"{record['concept_id']}")
            return (source_rank, record['concept_id'])
        records.sort(key=record_order)

        merged_attrs = {'aliases': set(), 'concept_id': '',
                        'trade_names': set(), 'xrefs': set()}
        set_fields = ['aliases', 'trade_names', 'xrefs']
        for record in records:
            for field in set_fields:
                if field in record:
                    merged_attrs[field] |= set(record[field])

            new_id_grp = f'{merged_attrs["concept_id"]}|{record["concept_id"]}'
            merged_attrs['concept_id'] = new_id_grp

            if 'label' not in merged_attrs \
                    and 'label' in record and record['label']:
                merged_attrs['label'] = record.get('label')

        for field in set_fields:
            merged_attrs[field] = list(merged_attrs[field])

        ref = f'{merged_attrs["concept_id"].lower()}##merger'
        merged_attrs['label_and_type'] = ref
        return merged_attrs
