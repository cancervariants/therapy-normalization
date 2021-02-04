"""Delete merged records from therapy_concepts DynamoDB Table."""
import sys
from pathlib import Path
import click
from timeit import default_timer as timer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(f"{PROJECT_ROOT}")

from therapy.database import Database  # noqa: E402


def delete_merged_records():
    """Scan the database to delete merged records."""
    db = Database()
    done = False
    start_key = None
    args = dict()
    while not done:
        if start_key:
            args['ExclusiveStartKey'] = start_key
        response = db.therapies.scan(**args)
        records = response['Items']

        with db.therapies.batch_writer(
                overwrite_by_pkeys=['label_and_type', 'concept_id']
        ) as batch:
            for record in records:
                if record['label_and_type'].endswith('##merger'):
                    batch.delete_item(
                        Key={
                            'label_and_type': record['label_and_type'],
                            'concept_id': record['concept_id']
                        }
                    )

        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None


if __name__ == '__main__':
    click.echo('Deleting merged records...')
    start = timer()
    delete_merged_records()
    end = timer()
    click.echo(f"Deleted merged records in {end-start} seconds.")
