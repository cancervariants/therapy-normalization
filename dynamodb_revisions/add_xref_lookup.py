"""Add xref reference to current concepts."""
import sys
from pathlib import Path
from timeit import default_timer as timer
import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(f"{PROJECT_ROOT}")

from therapy.database import Database  # noqa: E402


def add_xref_refs():
    """Add reference for xref attribute."""
    db = Database()
    batch = db.therapies.batch_writer()

    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = db.therapies.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = db.therapies.scan()
        last_evaluated_key = response.get('LastEvaluatedKey')

        records = response['Items']
        for record in records:
            if record['label_and_type'].endswith('##identity'):
                for xref in record.get('xrefs', []):
                    batch.put_item(Item={
                        'label_and_type': f"{xref.lower()}##xref",
                        'concept_id': record['concept_id'].lower(),
                        'src_name': record['src_name']
                    })

        if not last_evaluated_key:
            break


if __name__ == '__main__':
    click.echo("Adding xref references...")
    start = timer()
    add_xref_refs()
    end = timer()
    click.echo(f"finished adding xref references in {end - start:.5f}s.")
