"""This module provides a CLI util to make updates to normalizer database."""
import click
from botocore.exceptions import ClientError
from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt, ChemIDplus, RxNorm, Merge  # noqa: F401, E501
from therapy.schemas import SourceName
from timeit import default_timer as timer
from therapy.database import Database
from boto3.dynamodb.conditions import Key
import sys
from os import environ


MERGE_SOURCES = {SourceName.WIKIDATA.value, SourceName.RXNORM.value,
                 SourceName.NCIT.value, SourceName.CHEMIDPLUS.value}

SOURCES = {s.value.lower(): eval(s.value)
           for s in SourceName.__members__.values()}


class CLI:
    """Class for updating the normalizer database via Click"""

    @click.command()
    @click.option(
        '--normalizer',
        help="The normalizer(s) you wish to update separated by spaces."
    )
    @click.option(
        '--dev',
        is_flag=True,
        help="Working in development environment on localhost port 8000."
    )
    @click.option(
        '--db_url',
        help="URL endpoint for the application database."
    )
    @click.option(
        '--update_all',
        is_flag=True,
        help='Update all normalizer sources.'
    )
    def update_normalizer_db(normalizer, dev, db_url, update_all):
        """Update select normalizer source(s) in the therapy database."""
        if dev:
            db: Database = Database(db_url='http://localhost:8000')
        elif db_url:
            db: Database = Database(db_url=db_url)
        elif 'THERAPY_NORM_DB_URL' in environ.keys():
            # environment variable will be picked up by DB instance
            db: Database = Database()
        else:
            if click.confirm("Are you sure you want to update"
                             " the production database?", default=False):
                click.echo("Updating production db...")
                db: Database = Database()
            else:
                click.echo("Exiting CLI.")
                sys.exit()

        if update_all:
            normalizers = list(src for src in SOURCES)
            CLI()._update_normalizers(normalizers, db)
        elif not normalizer:
            CLI()._help_msg()
        else:
            normalizers = str(normalizer).lower().split()

            if len(normalizers) == 0:
                CLI()._help_msg()

            non_sources = CLI()._check_norm_srcs_match(normalizers)

            if len(non_sources) != 0:
                raise Exception(f"Not valid source(s): {non_sources}")

            CLI()._update_normalizers(normalizers, db)

    def _help_msg(self):
        """Display help message."""
        ctx = click.get_current_context()
        click.echo(
            "Must either enter 1 or more sources, or use `--update_all` parameter")  # noqa: E501
        click.echo(ctx.get_help())
        ctx.exit()

    def _check_norm_srcs_match(self, normalizers):
        """Check that entered normalizers are actual sources."""
        return set(normalizers) - {src for src in SOURCES}

    def _update_normalizers(self, normalizers, db):
        """Update selected normalizer sources."""
        processed_ids = []
        for n in normalizers:
            click.echo(f"\nDeleting {n}...")
            start_delete = timer()
            CLI()._delete_data(n, db)
            end_delete = timer()
            delete_time = end_delete - start_delete
            click.echo(f"Deleted {n} in "
                       f"{delete_time:.5f} seconds.\n")
            click.echo(f"Loading {n}...")
            start_load = timer()
            source = SOURCES[n](database=db)
            source_ids = source.perform_etl()
            if source.__class__.__name__ in MERGE_SOURCES:
                processed_ids += source_ids
            end_load = timer()
            load_time = end_load - start_load
            click.echo(f"Loaded {n} in {load_time:.5f} seconds.")
            click.echo(f"Total time for {n}: "
                       f"{(delete_time + load_time):.5f} seconds.")

        if processed_ids:
            click.echo("Generating merged concepts...")
            start = timer()
            merge = Merge(db)
            merge.create_merged_concepts(processed_ids)
            end = timer()
            click.echo(f"Generated merged concepts in {end - start:.5f} seconds.")  # noqa: E501

    def _delete_data(self, source, database):
        # Delete source's metadata
        try:
            metadata = database.metadata.query(
                KeyConditionExpression=Key(
                    'src_name').eq(SourceName[f"{source.upper()}"].value)
            )
            if metadata['Items']:
                database.metadata.delete_item(
                    Key={'src_name': metadata['Items'][0]['src_name']},
                    ConditionExpression="src_name = :src",
                    ExpressionAttributeValues={
                        ':src': SourceName[f"{source.upper()}"].value}
                )
        except ClientError as e:
            click.echo(e.response['Error']['Message'])

        # Delete source's data from therapies table
        try:
            while True:
                response = database.therapies.query(
                    IndexName='src_index',
                    KeyConditionExpression=Key('src_name').eq(
                        SourceName[f"{source.upper()}"].value)
                )

                records = response['Items']
                if not records:
                    break

                with database.therapies.batch_writer(
                        overwrite_by_pkeys=['label_and_type', 'concept_id']) \
                        as batch:

                    for record in records:
                        batch.delete_item(
                            Key={
                                'label_and_type': record['label_and_type'],
                                'concept_id': record['concept_id']
                            }
                        )
        except ClientError as e:
            click.echo(e.response['Error']['Message'])


if __name__ == '__main__':
    CLI().update_normalizer_db()
