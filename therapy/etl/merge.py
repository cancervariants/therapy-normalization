"""Create concept groups and merged records."""
from typing import Set, Dict, Optional
import logging
from timeit import default_timer as timer
import re

from therapy.schemas import SourcePriority
from therapy.database import Database


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Merge:
    """Handles record merging."""

    def __init__(self, database: Database) -> None:
        """Initialize Merge instance.

        self._groups is a dictionary keying concept IDs to the Set of concept
        IDs in that group. This is redundantly captured for all group members
        (i.e., a group of 5 concepts would all have their own key, and the
        value would be an identical Set of the same IDs)

        :param Database database: db instance to use for record retrieval
            and creation.
        """
        self._database = database
        self._groups: Dict[str, Set[str]] = {}

    def create_merged_concepts(self, record_ids: Set[str]) -> None:
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

        # don't create separate records for single-member groups
        self._groups = {k: v for k, v in self._groups.items() if len(v) > 1}

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

    def _get_xrefs(self, record: Dict) -> Set[str]:
        """Extract references to entries in other sources from a record.
        Combine xrefs with any Drugs@FDA records that can be gathered from
        UNII references.

        :param Dict record: record to process
        :return: Set of xref values
        :rtype: Set
        """
        xrefs = set()
        for xref in record.get('xrefs', []):
            xrefs.add(xref)
        uniis = [a for a in record.get('associated_with', [])
                 if a.startswith('unii:')]
        for unii in uniis:
            refs = self._database.get_records_by_type(unii.lower(),
                                                      "associated_with")

            def get_id(record: Dict) -> str:
                concept_id = record["concept_id"]
                return re.sub(r"([a]?nda)", lambda p: p.group(1).upper(),
                              concept_id)

            xrefs |= {get_id(r) for r in refs
                      if r['src_name'] == 'Drugs@FDA'}
        return xrefs

    def _rxnorm_brand_lookup(self, record_id: str) -> Optional[str]:
        """Rx Norm provides brand references back to original therapy concepts.
        This routine checks whether an ID from a concept group requires an
        additional dereference to obtain the original RxNorm record.
        :param str record_id: RxNorm concept ID to check
        :return: concept ID for RxNorm record if successful, None otherwise
        """
        brand_lookup = self._database.get_records_by_type(record_id,
                                                          'rx_brand')
        n = len(brand_lookup)
        if n == 1:
            lookup_id = brand_lookup[0]['concept_id']
            db_record = self._database.get_record_by_id(lookup_id,
                                                        False)
            if db_record:
                return db_record['concept_id']
        elif n > 1:
            logger.warning(f"Brand lookup for {record_id} had {n} matches")
        return None

    def _create_record_id_set(self, record_id: str,
                              observed_id_set: Set = set()) -> Set[str]:
        """Create concept ID group for an individual record ID.

        :param str record_id: concept ID for record to build group from
        :return: set of related identifiers pertaining to a common concept.
        """
        if record_id in self._groups:
            return self._groups[record_id]
        else:
            # get record
            db_record = self._database.get_record_by_id(record_id)
            if not db_record:
                if record_id.startswith('rxcui'):
                    brand_lookup = self._rxnorm_brand_lookup(record_id)
                    if brand_lookup:
                        return self._create_record_id_set(brand_lookup,
                                                          observed_id_set)
                logger.warning(f"Unable to resolve lookup for {record_id} in "
                               f"ID set: {observed_id_set}")
                return observed_id_set - {record_id}

            # construct group
            local_id_set = self._get_xrefs(db_record)
            if not local_id_set:
                return observed_id_set | {db_record['concept_id']}
            merged_id_set = {record_id} | observed_id_set
            for local_record_id in local_id_set - observed_id_set:
                merged_id_set |= self._create_record_id_set(local_record_id,
                                                            merged_id_set)
            return merged_id_set

    def _generate_merged_record(self, record_id_set: Set[str]) -> Dict:
        """Generate merged record from provided concept ID group.
        Where attributes are 'set-like', they should be combined, and where
        they are 'scalar-like', assign from the highest-priority source where
        that attribute is not None.

        Priority is:
        RxNorm > NCIt > HemOnc.org > DrugBank > GuideToPHARMACOLOGY >
            ChEMBL > ChemIDplus > Wikidata > Drugs@FDA

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
            if src == 'DRUGS@FDA':
                src = 'DRUGSATFDA'
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
            'xrefs': [r['concept_id'] for r in records[1:]],
            'label': None,
            'aliases': set(),
            'trade_names': set(),
            'associated_with': set(),
            'approval_status': set(),
            'approval_year': set(),
            'fda_indication': [],
        }

        # merge from constituent records
        set_fields = ['aliases', 'trade_names', 'associated_with',
                      'approval_year']
        for record in records:
            for field in set_fields:
                merged_attrs[field] |= set(record.get(field, set()))
            approval_status = record.get('approval_status')
            if approval_status:
                merged_attrs['approval_status'].add(approval_status)
            if merged_attrs['label'] is None:
                merged_attrs['label'] = record.get('label')
            for ind in record.get('fda_indication', []):
                if ind not in merged_attrs['fda_indication']:
                    merged_attrs['fda_indication'].append(ind)

        # clear unused fields
        for field in set_fields + ['fda_indication']:
            field_value = merged_attrs[field]
            if field_value:
                merged_attrs[field] = list(field_value)
            else:
                del merged_attrs[field]
        for field in ('label', 'approval_status'):
            if not merged_attrs[field]:
                del merged_attrs[field]

        merged_attrs['label_and_type'] = \
            f'{merged_attrs["concept_id"].lower()}##merger'
        merged_attrs['item_type'] = 'merger'
        return merged_attrs
