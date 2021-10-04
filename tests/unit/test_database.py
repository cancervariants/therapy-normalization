"""Test DynamoDB"""
import json
import os

import pytest
from boto3.dynamodb.conditions import Key

from therapy.database import Database
from tests.conftest import TEST_ROOT


@pytest.fixture(scope='module')
def db():
    """Create a DynamoDB test fixture."""

    class DB:
        def __init__(self):
            self.db = Database()
            if os.environ.get('TEST') is not None:
                self.load_test_data()

        def load_test_data(self):
            with open(f'{TEST_ROOT}/tests/unit/'
                      f'data/therapies.json', 'r') as f:
                therapies = json.load(f)
                with self.db.therapies.batch_writer() as batch:
                    for therapy in therapies:
                        batch.put_item(Item=therapy)
                f.close()

            with open(f'{TEST_ROOT}/tests/unit/'
                      f'data/metadata.json', 'r') as f:
                metadata = json.load(f)
                with self.db.metadata.batch_writer() as batch:
                    for m in metadata:
                        batch.put_item(Item=m)
                f.close()

    return DB().db


def test_tables_created(db):
    """Check that therapy_concepts and therapy_metadata are created."""
    existing_tables = db.dynamodb_client.list_tables()['TableNames']
    assert 'therapy_concepts' in existing_tables
    assert 'therapy_metadata' in existing_tables


def test_item_type(db):
    """Check that objects are tagged with item_type attribute."""
    filter_exp = Key('label_and_type').eq('chembl:chembl11359##identity')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'identity'

    filter_exp = Key('label_and_type').eq('interferon alfacon-1##label')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'label'

    filter_exp = Key('label_and_type').eq('unii:5x5hb3vz3z##associated_with')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'associated_with'

    filter_exp = Key('label_and_type').eq('drugbank:db00515##xref')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'xref'

    filter_exp = Key('label_and_type').eq('dichlorodiammineplatinum##alias')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'alias'

    filter_exp = Key('label_and_type').eq('rxcui:1041527##rx_brand')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'rx_brand'

    filter_exp = Key('label_and_type').eq('align##trade_name')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'trade_name'

    filter_exp = Key('label_and_type').eq('ncit:c839##merger')
    item = db.therapies.query(KeyConditionExpression=filter_exp)['Items'][0]
    assert 'item_type' in item
    assert item['item_type'] == 'merger'
