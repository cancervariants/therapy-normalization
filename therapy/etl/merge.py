"""Create concept groups and merged records."""
from therapy import PROHIBITED_SOURCES
from therapy.schemas import SourcePriority
from therapy.database import Database
from typing import Set, Dict
import logging
from timeit import default_timer as timer

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


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

        :param Set[str] record_ids: concept identifiers from which groups
            should be generated. Should *not* include any records from
            excluded sources.
        """
        logger.info('Generating record ID sets...')
        start = timer()
        for record_id in record_ids:
            new_group = self._create_record_id_set(record_id)
            if new_group:
                for concept_id in new_group:
                    self._groups[concept_id] = new_group
        end = timer()
        logger.debug(f'Built record ID sets in {end - start} seconds')

        logger.info('Creating merged records and updating database...')
        uploaded_ids = set()
        start = timer()
        for record_id, group in self._groups.items():
            if record_id in uploaded_ids:
                continue
            merged_record = self._generate_merged_record(group)

            # add group merger item to DB
            self._database.add_record(merged_record, 'merger')

            # add updated references
            for concept_id in group:
                if not self._database.get_record_by_id(concept_id, False):
                    logger.error(f"Updating nonexistent record: {concept_id} "
                                 f"for {merged_record['label_and_type']}")
                else:
                    merge_ref = merged_record['concept_id'].lower()
                    self._database.update_record(concept_id, 'merge_ref',
                                                 merge_ref)
            uploaded_ids |= group
        logger.info('Merged concept generation successful.')
        end = timer()
        logger.debug(f'Generated and added concepts in {end - start} seconds')

    def _get_other_ids(self, record: Dict) -> Set[str]:
        """Extract references to entries in other sources from a record.
        Crucially, filter to allowed sources only.

        :param Dict record: record to process
        :return: Set of other_identifier values
        :rtype: Set
        """
        other_ids = set()
        for other_id in record.get('other_identifiers', []):
            if other_id.split(':')[0] not in PROHIBITED_SOURCES:
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
                # attempt RxNorm brand lookup
                brand_lookup = self._database.get_records_by_type(record_id,
                                                                  'rx_brand')
                if len(brand_lookup) == 1:
                    lookup_id = brand_lookup[0]['concept_id']
                    db_record = self._database.get_record_by_id(lookup_id,
                                                                False)
                    if db_record:
                        record_id = db_record['concept_id']
                        return self._create_record_id_set(record_id,
                                                          observed_id_set)
                    else:
                        return observed_id_set - {record_id}

                else:
                    logger.warning(f"Record ID set creator could not resolve "
                                   f"lookup for {record_id} in ID set: "
                                   f"{observed_id_set}")
                    return observed_id_set - {record_id}

            local_id_set = self._get_other_ids(db_record)
            if not local_id_set:
                return observed_id_set | {db_record['concept_id']}
            merged_id_set = {record_id} | observed_id_set
            for local_record_id in local_id_set - observed_id_set:
                merged_id_set |= self._create_record_id_set(local_record_id,
                                                            merged_id_set)
            return merged_id_set

    def _generate_merged_record(self, record_id_set: Set[str]) -> Dict:
        """Generate merged record from provided concept ID group.
        Where attributes are sets, they should be merged, and where they are
        scalars, assign from the highest-priority source where that attribute
        is non-null.

        Priority is RxNorm > NCIt > ChemIDplus > Wikidata. ChEMBL and DrugBank
        identifiers should not be included in record_id_set; doing so will
        cause problems.

        :param Set record_id_set: group of concept IDs
        :return: completed merged drug object to be stored in DB
        """
        records = []
        for record_id in record_id_set:
            record = self._database.get_record_by_id(record_id)
            if record:
                records.append(record)
            else:
                logger.error(f"Merge record generator could not retrieve "
                             f"record for {record_id} in {record_id_set}")

        def record_order(record):
            """Provide priority values of concepts for sort function."""
            src = record['src_name'].upper()
            if src in SourcePriority.__members__:
                source_rank = SourcePriority[src].value
            else:
                raise Exception(f"Prohibited source: {src} in concept_id "
                                f"{record['concept_id']}")
            return (source_rank, record['concept_id'])
        records.sort(key=record_order)

        # initialize merged record
        merged_attrs = {
            'concept_id': records[0]['concept_id'],
            'label': None,
            'aliases': set(),
            'trade_names': set(),
            'xrefs': set()
        }
        if len(records) > 1:
            merged_attrs['other_ids'] = [r['concept_id'] for r in records[1:]]

        # merge from constituent records
        set_fields = ['aliases', 'trade_names', 'xrefs']
        for record in records:
            for field in set_fields:
                merged_attrs[field] |= set(record.get(field, {}))
            if merged_attrs['label'] is None:
                merged_attrs['label'] = record.get('label')

        # clear unused fields
        for field in set_fields:
            field_value = merged_attrs[field]
            if field_value:
                merged_attrs[field] = list(field_value)
            else:
                del merged_attrs[field]
        if merged_attrs['label'] is None:
            del merged_attrs['label']

        merged_attrs['label_and_type'] = \
            f'{merged_attrs["concept_id"].lower()}##merger'
        return merged_attrs
