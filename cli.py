"""This module provides a CLI util to make updates to normalizer database."""
import click
from botocore.exceptions import ClientError
from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt
from therapy.schemas import SourceName
from timeit import default_timer as timer
from therapy.database import THERAPIES_TABLE, METADATA_TABLE
from boto3.dynamodb.conditions import Key


class CLI:
    """Class for updating the normalizer database via Click"""

    @click.command()
    @click.option(
        '--all',
        is_flag=True,
        help='Update all normalizer databases.'
    )
    @click.option(
        '--normalizer',
        help="The normalizer(s) you wish to update separated by spaces."
    )
    def update_normalizer_db(normalizer, all):
        """Update select normalizer(s) sources in the therapy database."""
        sources = {
            'chembl': ChEMBL,
            'ncit': NCIt,
            'wikidata': Wikidata,
            'drugbank': DrugBank
        }

        normalizers = normalizer.lower().split()
        if len(normalizers) == 0:
            raise Exception("Must enter a normalizer.")
        for n in normalizers:
            if n in sources:
                click.echo(f"\nDeleting {n}...")
                start_delete = timer()
                CLI()._delete_data(n)
                end_delete = timer()
                delete_time = end_delete - start_delete
                click.echo(f"Deleted {n} in "
                           f"{delete_time} seconds.\n")
                click.echo(f"Loading {n}...")
                start_load = timer()
                sources[n]()
                end_load = timer()
                load_time = end_load - start_load
                click.echo(f"Loaded {n} in {load_time} seconds.")
                click.echo(f"Total time for {n}: "
                           f"{delete_time + load_time} seconds.")
            else:
                raise Exception("Not a normalizer source.")

    def _delete_data(self, source):
        # Delete source's metadata
        try:
            metadata = METADATA_TABLE.query(
                KeyConditionExpression=Key(
                    'src_name').eq(SourceName[f"{source.upper()}"].value)
            )
            if metadata['Items']:
                METADATA_TABLE.delete_item(
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
                response = THERAPIES_TABLE.query(
                    IndexName='src_index',
                    KeyConditionExpression=Key('src_name').eq(
                        SourceName[f"{source.upper()}"].value)
                )

                records = response['Items']
                if not records:
                    break

                with THERAPIES_TABLE.batch_writer(
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
