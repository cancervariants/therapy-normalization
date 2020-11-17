"""This module provides a CLI util to make updates to normalizer database."""
import click
from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt  # noqa: F401
from therapy.schemas import SourceName
from timeit import default_timer as timer
from therapy.database import Database, DYNAMODB, DYNAMODBCLIENT
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
            'ncit': NCIt,
            'chembl': ChEMBL,
            'wikidata': Wikidata,
            'drugbank': DrugBank,
        }

        if all:
            CLI()._delete_all_data()
            Database()
            for n in sources:
                # CLI()._delete_data(n, DYNAMODB)
                click.echo(f"Loading {n}...")
                start = timer()
                sources[n]()
                end = timer()
                click.echo(f"Loaded {n} in {end - start} seconds.")
            click.echo('Finished updating all normalizer sources.')
        else:
            Database()
            normalizers = normalizer.lower().split()
            if len(normalizers) == 0:
                raise Exception("Must enter a normalizer.")
            for n in normalizers:
                if n in sources:
                    # TODO: Fix so that self._delete_data(n) works
                    CLI()._delete_data(n, DYNAMODB)
                    click.echo(f"Loading {n}...")
                    start = timer()
                    # sources[n]()
                    end = timer()
                    click.echo(f"Loaded {n} in {end - start} seconds.")
                else:
                    raise Exception("Not a normalizer source.")

    def _delete_all_data(self):
        tables = DYNAMODBCLIENT.list_tables()['TableNames']
        for table in tables:
            response = DYNAMODBCLIENT.delete_table(TableName=table)  # noqa F841
            print(f"Deleted table: {table}")

    def _delete_data(self, source, dynamodb):
        click.echo(f"Start deleting the {source} source.")

        therapies_table = dynamodb.Table('Therapies')  # noqa F841
        metadata_table = dynamodb.Table('MetaData')  # noqa F841

        try:
            while True:
                response = therapies_table.query(
                    IndexName='src_index',
                    KeyConditionExpression=Key(
                        'src_name').eq(SourceName[f"{source.upper()}"].value)
                )
                records = response['Items']
                if not records:
                    break
                for record in records:
                    therapies_table.delete_item(
                        Key={
                            'label_and_type': record['label_and_type'],
                            'concept_id': record['concept_id']
                        },
                        ConditionExpression="begins_with(concept_id, :src)",
                        ExpressionAttributeValues={':src': source.lower()}
                    )

            metadata = metadata_table.query(
                KeyConditionExpression=Key(
                    'src_name').eq(SourceName[f"{source.upper()}"].value)
            )
            metadata = metadata['Items'][0]['src_name']
            metadata_table.delete_item(
                Key={'src_name': metadata},
                ConditionExpression="src_name = :src",
                ExpressionAttributeValues={
                    ':src': SourceName[f"{source.upper()}"].value}
            )
        except ValueError:
            print("Not a valid query.")

        click.echo(f"Finished deleting the {source} source.")


if __name__ == '__main__':
    CLI().update_normalizer_db()
