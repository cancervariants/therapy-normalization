"""Test DynamoDB"""
import pytest
from therapy.database import Database


@pytest.fixture(scope='module')
def db():
    """Create a DynamoDB test fixture."""
    return Database(db_url='http://localhost:8000')


def test_tables_created(db):
    """Check that therapy_concepts and therapy_metadata are created."""
    existing_tables = db.ddb_client.list_tables()['TableNames']
    assert 'therapy_concepts' in existing_tables
    assert 'therapy_metadata' in existing_tables
