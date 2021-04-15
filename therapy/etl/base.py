"""A base class for extraction, transformation, and loading of data."""
from abc import ABC, abstractmethod
from typing import List, Dict
from therapy import ACCEPTED_SOURCES
from therapy.schemas import Therapy
import logging


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Base(ABC):
    """The ETL base class."""

    def __init__(self, database):
        """Extract from sources."""
        self.database = database
        self._in_normalize = self.__class__.__name__.lower() \
            in ACCEPTED_SOURCES
        if self._in_normalize:
            self._processed_ids = []

    @abstractmethod
    def perform_etl(self) -> List[str]:
        """Public-facing method to begin ETL procedures on given data.

        Returned concept IDs can be passed to Merge method for computing
        merged concepts.

        :return: list of concept IDs which were successfully processed and
            uploaded.
        """
        raise NotImplementedError

    @abstractmethod
    def _download_data(self, *args, **kwargs):
        raise NotImplementedError

    def _extract_data(self):
        """Get source file from data directory."""
        self._src_data_dir.mkdir(exist_ok=True, parents=True)
        src_file_prefix = f'{type(self).__name__.lower()}_'
        dir_files = [f for f in self._src_data_dir.iterdir()
                     if f.name.startswith(src_file_prefix)]
        if len(dir_files) == 0:
            self._download_data()
            dir_files = [f for f in self._src_data_dir.iterdir()
                         if f.name.startswith(src_file_prefix)]
        self._src_file = sorted(dir_files, reverse=True)[0]
        self._version = self._src_file.stem.split('_', 1)[1]

    @abstractmethod
    def _transform_data(self, *args, **kwargs):
        raise NotImplementedError

    def _load_therapy(self, therapy: Dict):
        """Load individual therapy record."""
        assert Therapy(**therapy)
        concept_id = therapy['concept_id']

        field_pairs = (('aliases', 'alias'), ('other_identifiers', 'other_id'),
                       ('xrefs', 'xref'), ('trade_names', 'trade_name'))
        for field, field_name in field_pairs:
            if field in therapy:
                items = therapy[field]
                if items == [] or items is None:
                    del therapy[field]
                else:
                    items = {i.casefold() for i in items}
                    if field == 'aliases':
                        if len(items) > 20:
                            logger.debug(f"{concept_id} has > 20 {field}.")
                            del therapy[field]
                            continue
                    for item in items:
                        self.database.add_ref_record(item, concept_id,
                                                     field_name)

        if 'approval_status' in therapy \
                and therapy['approval_status'] is None:
            del therapy['approval_status']
        if 'label' in therapy:
            label = therapy['label']
            if label:
                self.database.add_ref_record(label, concept_id, 'label')
            else:
                del therapy['label']

        self.database.add_record(therapy)
        if self._in_normalize:
            self._processed_ids.append(concept_id)

    @abstractmethod
    def _load_meta(self, *args, **kwargs):
        raise NotImplementedError
