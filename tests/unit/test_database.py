"""Test DynamoDB"""
import pytest
from therapy.database import Database
import json
import os


@pytest.fixture(scope='module')
def db(provide_root):
    """Create a DynamoDB test fixture."""
    TEST_ROOT = provide_root

    class DB:
        def __init__(self):
            self.db = Database()
            if os.environ.get('TEST') is not None:
                self.load_test_data()

        def load_test_data(self):
            with open(TEST_ROOT / 'tests' / 'unit' / 'data' / 'therapies.json',
                      'r') as f:
                therapies = json.load(f)
                with self.db.therapies.batch_writer() as batch:
                    for therapy in therapies:
                        batch.put_item(Item=therapy)
                f.close()

            with open(TEST_ROOT / 'tests' / 'unit' / 'data' / 'metadata.json',
                      'r') as f:
                metadata = json.load(f)
                with self.db.metadata.batch_writer() as batch:
                    for m in metadata:
                        batch.put_item(Item=m)
                f.close()

    return DB()


def test_tables_created(db):
    """Check that therapy_concepts and therapy_metadata are created."""
    existing_tables = db.db.dynamodb_client.list_tables()['TableNames']
    assert 'therapy_concepts' in existing_tables
    assert 'therapy_metadata' in existing_tables
