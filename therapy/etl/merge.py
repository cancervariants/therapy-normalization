"""Create concept groups and merged records."""
from therapy.schemas import MergedDrug
from therapy.database import Database
from typing import Set
import logging

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Merge:
    """Handles record merging."""

    def __init__(self, database: Database):
        """Initialize Merge instance.

        :param Database database: db instance to use for record retrieval
            and creation.
        """
        self.database = database

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
         * When computing groups, how to handle cases where new group
           additions are discovered in subsequent passes?
        """
        raise NotImplementedError

    def _create_record_id_set(self, record_id: str,
                              observed_id_set: Set = set()) -> Set[str]:
        """Create concept ID group.

        :param str record_id: concept ID for record to build group from
        :return: set of related identifiers pertaining to a common concept.
        """
        raise NotImplementedError

    def _generate_merged_record(self, record_id_set: Set) -> MergedDrug:
        """Generate merged record from provided concept ID group.
        Where attributes are sets, they should be merged, and where they are
        scalars, assign from the highest-priority source where that attribute
        is non-null. Priority is NCIt > ChEMBL > DrugBank > Wikidata.

        :param Set record_id_set: group of concept IDs
        :return: completed DBMergedRecord object
        """
        raise NotImplementedError
