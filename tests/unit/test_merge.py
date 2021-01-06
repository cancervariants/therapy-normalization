"""Test merged record generation."""
import pytest
from therapy.etl.merge import Merge
from therapy.database import Database


@pytest.fixture(scope='module')
def get_merge():
    """Provide Merge instance to test cases."""
    class MergeHandler():
        def __init__(self):
            self.merge = Merge(Database())

        def create_merged_concepts(self, record_ids):
            return self.merge.create_merged_concepts(record_ids)

        def create_record_id_set(self, record_id):
            return self.merge._create_record_id_set(record_id)

        def generate_merged_record(self, record_id_set):
            return self.merge._generate_merged_record(record_id_set)
    return MergeHandler()


@pytest.fixture(scope='module')
def phenobarbital():
    """Create phenobarbital fixture."""
    pass


def test_merge(get_merge, phenobarbital):
    """Test end-to-end merge function."""
    assert True
