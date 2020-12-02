"""This module provides a CLI util to make updates to normalizer database."""
import click
from botocore.exceptions import ClientError
from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt
from therapy.schemas import SourceName
from timeit import default_timer as timer
from therapy.database import Database
from boto3.dynamodb.conditions import Key


class CLI:
    """Class for updating the normalizer database via Click"""

    @click.command()
    @click.option(
        '--normalizer',
        help="The normalizer(s) you wish to update separated by commas."
    )
    def update_normalizer_db(normalizer, update_all):
        """Update select normalizer(s) sources in the therapy database."""
        sources = {
            'chembl': ChEMBL,
            'ncit': NCIt,
            'wikidata': Wikidata,
            'drugbank': DrugBank
        }

        db: Database = Database()

        normalizers = normalizer.lower().split(',')

        if len(normalizers) == 0:
            raise Exception("Must enter a normalizer.")
        if update_all and normalizer:
            raise click.UsageError("Cannot specify normalizers and call --all")
        if update_all:
            normalizers = sources

        for n in normalizers:
            if n in sources:
                click.echo(f"\nDeleting {n}...")
                start_delete = timer()
                CLI()._delete_data(n, db)
                end_delete = timer()
                delete_time = end_delete - start_delete
                click.echo(f"Deleted {n} in "
                           f"{delete_time:.5f} seconds.\n")
                click.echo(f"Loading {n}...")
                start_load = timer()
                sources[n](database=db)
                end_load = timer()
                load_time = end_load - start_load
                click.echo(f"Loaded {n} in {load_time:.5f} seconds.")
                click.echo(f"Total time for {n}: "
                           f"{(delete_time + load_time):.5f} seconds.")
            else:
                raise Exception("Not a normalizer source.")

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
