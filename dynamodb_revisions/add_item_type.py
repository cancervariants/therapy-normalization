"""Add item_type attribute to all items."""
import sys
from pathlib import Path
from timeit import default_timer as timer
import click
from botocore.exceptions import ClientError
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(f"{PROJECT_ROOT}")

from therapy.database import Database  # noqa: E402


logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)

db = Database()


def add_item_type(label_and_type: str, concept_id: str, item_type: str):
    """Add item_type to individual db item."""
    key = {
        'label_and_type': label_and_type,
        'concept_id': concept_id
    }
    update_expression = "set item_type=:r"
    update_values = {':r': item_type}
    try:
        db.therapies.update_item(Key=key,
                                 UpdateExpression=update_expression,
                                 ExpressionAttributeValues=update_values)
    except ClientError as e:
        logger.error(f"boto3 client error in `database.update_record()`: "
                     f"{e.response['Error']['Message']}")


def add_item_types():
    """Add item_type attribute to all items."""
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = db.therapies.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = db.therapies.scan()

        records = response['Items']
        for record in records:
            label_and_type = record['label_and_type']
            if label_and_type.endswith('##identity'):
                add_item_type(label_and_type, record['concept_id'],
                              'identity')
            elif label_and_type.endswith('##label'):
                add_item_type(label_and_type, record['concept_id'], 'label')
            elif label_and_type.endswith('##trade_name'):
                add_item_type(label_and_type, record['concept_id'],
                              'trade_name')
            elif label_and_type.endswith('##rx_brand'):
                add_item_type(label_and_type, record['concept_id'], 'rx_brand')
            elif label_and_type.endswith('##alias'):
                add_item_type(label_and_type, record['concept_id'], 'alias')
            elif label_and_type.endswith('##other_id'):
                add_item_type(label_and_type, record['concept_id'], 'other_id')
            elif label_and_type.endswith('##xref'):
                add_item_type(label_and_type, record['concept_id'], 'xref')
            elif label_and_type.endswith('##merger'):
                add_item_type(label_and_type, record['concept_id'], 'merger')
            else:
                logger.error(f"Couldn't parse item type for record: {record}")

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break


if __name__ == '__main__':
    click.echo("Adding item_types...")
    start = timer()
    add_item_types()
    end = timer()
    click.echo(f"finished adding item_types in {end - start:.5f}s.")
