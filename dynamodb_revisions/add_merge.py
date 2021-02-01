"""Add merged records and references to database."""
from therapy.database import Database
from therapy.etl.merge import Merge
from timeit import default_timer as timer
from therapy.schemas import SourceName
from os import environ
import click
import sys


def main():
    """Execute scan on DB to gather concept IDs and add merged concepts."""
    start = timer()
    db = Database()

    print("Compiling concept IDs...")
    concept_ids = []
    done = False
    start_key = None
    args = dict()
    while not done:
        if start_key:
            args['ExclusiveStartKey'] = start_key
        response = db.therapies.scan(**args)
        items = response['Items']
        for item in items:
            if item['label_and_type'].endswith('##identity'):
                if item['src_name'] != SourceName.CHEMBL.value and \
                        item['src_name'] != SourceName.DRUGBANK.value:
                    concept_ids.append(item['concept_id'])

        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    print("Concept ID scan successful.")
    print(f"{len(concept_ids)} concept IDs gathered.")

    merge = Merge(db)

    merge.create_merged_concepts(concept_ids)
    end = timer()
    print(f"Merge generation successful, time: {end - start}")


if __name__ == '__main__':
    if 'THERAPY_NORM_DB_URL' not in environ.keys():
        if click.confirm("Are you sure you want to update"
                         " the production database?", default=False):
            click.echo("Updating production db...")
        else:
            click.echo("Exiting.")
            sys.exit()
    main()
