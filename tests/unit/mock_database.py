"""Fake database object for use in test cases."""
from therapy import PROJECT_ROOT
from therapy.Database import Database
from typing import Dict, Any, Optional
import json


class MockDatabase(Database):
    """`Mock` database object to use in test cases."""

    def __init__(self):
        """Initialize mock database object.

        self.records contains
        """
        infile = PROJECT_ROOT / 'tests' / 'unit' / 'data' / 'therapies.json'
        with open(infile, 'r') as f:
            self.records = json.load(f)
        self.updates = {}

    def get_record_by_id(self, record_id: str) -> Optional[Dict]:
        """Fetch record corresponding to provided concept ID

        :param str concept_id: concept ID for therapy record
        :param bool case_sensitive: if true, performs exact lookup, which is
            more efficient. Otherwise, performs filter operation, which
            doesn't require correct casing.
        :return: complete therapy record, if match is found; None otherwise
        """
        key = f'{record_id.lower}##identity'
        for record in self.records:
            if record['label_and_type'] == key:
                return record
        # TESTING TODO REMOVE
        print(record_id)
        return None

    def update_record(self, concept_id: str, attribute: str, value: Any):
        """Store update request sent to database.

        :param str concept_id: record to update
        :param str field: name of field to update
        :parm str value: new value
        """
        self.updates[concept_id] = {
            'concept_id': concept_id,
            attribute: value
        }
