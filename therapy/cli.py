"""This module provides a CLI util to make updates to normalizer database."""
import click
from botocore.exceptions import ClientError
from therapy import ACCEPTED_SOURCES
from therapy.etl.merge import Merge
from timeit import default_timer as timer
from boto3.dynamodb.conditions import Key
from therapy.schemas import SourceName
from therapy import SOURCES_CLASS, SOURCES
from therapy.database import Database
from disease.database import Database as DiseaseDatabase
from disease.cli import CLI as DiseaseCLI
from disease.schemas import SourceName as DiseaseSources
from os import environ
import logging

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class CLI:
    """Class for updating the normalizer database via Click"""

    @staticmethod
    @click.command()
    @click.option(
        '--normalizer',
        help="The normalizer(s) you wish to update separated by spaces."
    )
    @click.option(
        '--prod',
        is_flag=True,
        help="Working in production environment."
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
    @click.option(
        '--update_merged',
        is_flag=True,
        help='Update concepts for normalize endpoint from accepted sources.'
    )
    def update_normalizer_db(normalizer, prod, db_url, update_all,
                             update_merged):
        """Update select normalizer source(s) in the therapy database."""
        endpoint_url = None
        if prod:
            environ['THERAPY_NORM_PROD'] = "TRUE"
            environ['DISEASE_NORM_PROD'] = "TRUE"
            db: Database = Database()
        else:
            if db_url:
                endpoint_url = db_url
            elif 'THERAPY_NORM_DB_URL' in environ.keys():
                endpoint_url = environ['THERAPY_NORM_DB_URL']
            else:
                endpoint_url = 'http://localhost:8000'
            db: Database = Database(db_url=endpoint_url)

        if update_all:
            normalizers = list(src for src in SOURCES)
            CLI()._check_disease_normalizer(normalizers, endpoint_url)
            CLI()._update_normalizers(normalizers, db, update_merged)
        elif not normalizer:
            CLI()._help_msg()
        else:
            normalizers = str(normalizer).lower().split()

            if len(normalizers) == 0:
                CLI()._help_msg()

            non_sources = set(normalizers) - {src for src in SOURCES}

            if len(non_sources) != 0:
                raise Exception(f"Not valid source(s): {non_sources}")

            CLI()._check_disease_normalizer(normalizers, endpoint_url)
            CLI()._update_normalizers(normalizers, db, update_merged)

    def _check_disease_normalizer(self, normalizers, endpoint_url):
        """When loading HemOnc source, perform rudimentary check of Disease
            Normalizer tables, and reload them if necessary.

        :param list normalizers: List of sources to load
        :param str endpoint_url: Therapy endpoint URL
        """
        def _load_diseases():
            msg = "Disease Normalizer not loaded. " \
                  "Loading Disease Normalizer..."
            logger.debug(msg)
            click.echo(msg)
            try:
                DiseaseCLI().update_normalizer_db(
                    ['--update_all', '--update_merged',
                     '--db_url', endpoint_url]
                )
            except Exception as e:
                logger.error(e)
                raise Exception(e)
            except:  # noqa: E722
                pass

        if 'THERAPY_NORM_PROD' not in environ and 'hemonc' in normalizers:
            db = DiseaseDatabase(db_url=endpoint_url)
            current_tables = {table.name for table in db.dynamodb.tables.all()}
            if not all(name in current_tables
                       for name in ('disease_concepts', 'disease_metadata')):
                _load_diseases()
            elif db.diseases.scan()['Count'] == 0 or \
                    db.metadata.scan()['Count'] < len(DiseaseSources):
                _load_diseases()

    @staticmethod
    def _help_msg():
        """Display help message."""
        ctx = click.get_current_context()
        click.echo(
            "Must either enter 1 or more sources, or use `--update_all` parameter")  # noqa: E501
        click.echo(ctx.get_help())
        ctx.exit()

    @staticmethod
    def _update_normalizers(normalizers, db, update_merged):
        """Update selected normalizer sources."""
        processed_ids = list()
        for n in normalizers:
            msg = f"Deleting {n}..."
            click.echo(f"\n{msg}")
            logger.info(msg)
            start_delete = timer()
            CLI()._delete_data(n, db)
            end_delete = timer()
            delete_time = end_delete - start_delete
            msg = f"Deleted {n} in {delete_time:.5f} seconds."
            click.echo(f"{msg}\n")
            logger.info(msg)

            msg = f"Loading {n}..."
            click.echo(msg)
            logger.info(msg)
            start_load = timer()
            source = SOURCES_CLASS[n](database=db)
            if source.in_normalize:
                processed_ids += source.perform_etl()
            else:
                source.perform_etl()
            end_load = timer()
            load_time = end_load - start_load
            msg = f"Loaded {n} in {load_time:.5f} seconds."
            click.echo(msg)
            logger.info(msg)
            msg = f"Total time for {n}: " \
                  f"{(delete_time + load_time):.5f} seconds."
            click.echo(msg)
            logger.info(msg)

        if update_merged:
            start_merge = timer()
            if not processed_ids or not ACCEPTED_SOURCES.issubset(normalizers):
                CLI()._delete_normalized_data(db)
                processed_ids = db.get_ids_for_merge()
            merge = Merge(database=db)
            click.echo("Constructing normalized records...")
            merge.create_merged_concepts(processed_ids)
            end_merge = timer()
            click.echo(f"Merged concept generation completed in"
                       f" {(end_merge - start_merge):.5f} seconds.")

    @staticmethod
    def _delete_normalized_data(database):
        click.echo("\nDeleting normalized records...")
        start_delete = timer()
        try:
            while True:
                with database.therapies.batch_writer(
                        overwrite_by_pkeys=['label_and_type', 'concept_id']) \
                        as batch:
                    response = database.therapies.query(
                        IndexName='item_type_index',
                        KeyConditionExpression=Key('item_type').eq('merger'),
                    )
                    records = response['Items']
                    if not records:
                        break
                    for record in records:
                        batch.delete_item(Key={
                            'label_and_type': record['label_and_type'],
                            'concept_id': record['concept_id']
                        })
        except ClientError as e:
            click.echo(e.response['Error']['Message'])
        end_delete = timer()
        delete_time = end_delete - start_delete
        click.echo(f"Deleted normalized records in {delete_time:.5f} seconds.")

    @staticmethod
    def _delete_data(source, database):
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

        try:
            while True:
                response = database.therapies.query(
                    IndexName='src_index',
                    KeyConditionExpression=Key('src_name').eq(
                        SourceName[f"{source.upper()}"].value),
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
