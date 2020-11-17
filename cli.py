"""This module provides a CLI util to make updates to normalizer database."""
import click
from botocore.exceptions import ClientError
from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt
from therapy.schemas import SourceName
from timeit import default_timer as timer
from therapy.database import Database, DYNAMODB, THERAPIES_TABLE, \
    METADATA_TABLE
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
        DB = Database()

        sources = {
            'chembl': ChEMBL,
            'ncit': NCIt,
            'wikidata': Wikidata,
            'drugbank': DrugBank
        }

        if all:
            CLI()._delete_all_data(DB)
            tables = DB.db_client.list_tables()['TableNames']
            DB.create_meta_data_table(tables)
            tables = DB.db_client.list_tables()['TableNames']
            DB.create_therapies_table(tables)
            for n in sources:
                CLI()._delete_data(n, DYNAMODB)
                click.echo(f"Loading {n}...")
                start = timer()
                sources[n]()
                end = timer()
                click.echo(f"Loaded {n} in {end - start} seconds.")
            click.echo('Finished updating all normalizer sources.')
        else:
            normalizers = normalizer.lower().split()
            if len(normalizers) == 0:
                raise Exception("Must enter a normalizer.")
            for n in normalizers:
                if n in sources:
                    CLI()._delete_data(n)
                    click.echo(f"Loading {n}...")
                    start = timer()
                    sources[n]()
                    end = timer()
                    click.echo(f"Loaded {n} in {end - start} seconds.")
                else:
                    raise Exception("Not a normalizer source.")

    def _delete_all_data(self, db):
        tables = db.db_client.list_tables()['TableNames']
        for table in tables:
            response = db.db_client.delete_table(TableName=table)  # noqa F841
            click.echo(f"Deleted table: {table}")

    def _delete_data(self, source):
        click.echo(f"Start deleting the {source} source.")

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
        # TODO: Sometimes works? Sometimes leaves < 10 items left?
        try:
            with THERAPIES_TABLE.batch_writer(
                    overwrite_by_pkeys=['label_and_type', 'concept_id']) \
                    as batch:
                while True:
                    response = THERAPIES_TABLE.query(
                        IndexName='src_index',
                        KeyConditionExpression=Key(
                            'src_name').eq(
                            SourceName[f"{source.upper()}"].value)
                    )
                    records = response['Items']
                    if not records:
                        break
                    for record in records:
                        batch.delete_item(
                            Key={
                                'label_and_type': record['label_and_type'],
                                'concept_id': record['concept_id']
                            }
                        )
        except ClientError as e:
            click.echo(e.response['Error']['Message'])

        click.echo(f"Finished deleting the {source} source.")


if __name__ == '__main__':
    CLI().update_normalizer_db()
