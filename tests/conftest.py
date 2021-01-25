"""Pytest test config tools."""
from therapy.database import Database
from therapy import PROJECT_ROOT
from typing import Dict, Any, Optional
import json
import pytest


@pytest.fixture(scope='module')
def mock_database():
    """Return MockDatabase object."""

    class MockDatabase(Database):
        """Mock database object to use in test cases."""

        def __init__(self):
            """Initialize mock database object. This class's method's shadow the
            actual Database class methods.

            `self.records` loads preexisting DB items.
            `self.added_records` stores add record requests, with the
            concept_id as the key and the complete record as the value.
            `self.updates` stores update requests, with the concept_id as the
            key and the updated attribute and new value as the value.
            """
            infile = PROJECT_ROOT / 'tests' / 'unit' / 'data' / 'therapies.json'  # noqa: E501
            self.records = {}
            with open(infile, 'r') as f:
                records_json = json.load(f)
                for record in records_json:
                    self.records[record['label_and_type']] = {
                        record['concept_id']: record
                    }
            self.added_records: Dict[str, Dict[Any, Any]] = {}
            self.updates: Dict[str, Dict[Any, Any]] = {}

        def get_record_by_id(self, record_id: str) -> Optional[Dict]:
            """Fetch record corresponding to provided concept ID.

            :param str concept_id: concept ID for therapy record
            :param bool case_sensitive: if true, performs exact lookup, which
                is more efficient. Otherwise, performs filter operation, which
                doesn't require correct casing.
            :return: complete therapy record, if match is found; None otherwise
            """
            label_and_type = f'{record_id.lower()}##identity'
            record_lookup = self.records.get(label_and_type, None)
            if record_lookup:
                return record_lookup.get(record_id, None)
            else:
                return None

        def add_record(self, record: Dict, record_type: str):
            """Store add record request sent to database.

            :param Dict record: record (of any type) to upload. Must include
                `concept_id` key. If record is of the `identity` type, the
                concept_id must be correctly-cased.
            :param str record_type: ignored by this function
            """
            self.added_records[record['concept_id']] = record

        def update_record(self, concept_id: str, attribute: str,
                          new_value: Any):
            """Store update request sent to database.

            :param str concept_id: record to update
            :param str field: name of field to update
            :parm str new_value: new value
            """
            self.updates[concept_id] = {attribute: new_value}

    return MockDatabase
