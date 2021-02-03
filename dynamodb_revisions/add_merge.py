"""Add merged records and references to database."""
from timeit import default_timer as timer
from pathlib import Path
from os import environ
import click
import sys
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(f"{PROJECT_ROOT}")

from therapy.database import Database  # noqa: E402
from therapy.etl.merge import Merge  # noqa: E402
from therapy import ACCEPTED_SOURCES  # noqa: #402

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


def main():
    """Execute scan on DB to gather concept IDs and add merged concepts."""
    start = timer()
    db = Database()

    logger.info("Compiling concept IDs...")
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
                if item['src_name'].lower() in ACCEPTED_SOURCES:
                    concept_ids.append(item['concept_id'])

        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    logger.info("Concept ID scan successful.")
    logger.info(f"{len(concept_ids)} concept IDs gathered.")

    merge = Merge(db)

    merge.create_merged_concepts(concept_ids)
    end = timer()
    logger.info(f"Merge generation successful, time: {end - start}")


if __name__ == '__main__':
    if 'THERAPY_NORM_DB_URL' not in environ.keys():
        if click.confirm("Are you sure you want to update"
                         " the production database?", default=False):
            click.echo("Updating production db...")
        else:
            click.echo("Exiting.")
            sys.exit()
    main()
