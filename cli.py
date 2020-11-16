"""This module provides a CLI util to make updates to normalizer database."""
import click
# from therapy.etl import ChEMBL, Wikidata, DrugBank, NCIt  # noqa: F401
from therapy.etl import Wikidata, NCIt
from therapy.schemas import SourceName
from timeit import default_timer as timer
from therapy.database import DB
import boto3  # noqa: F401


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
            # 'chembl': ChEMBL,
            'wikidata': Wikidata,
            # 'drugbank': DrugBank,
        }

        if all:
            CLI()._delete_all_data(DB)
            tables = DB.db_client.list_tables()['TableNames']
            DB.create_meta_data_table(tables)
            tables = DB.db_client.list_tables()['TableNames']
            DB.create_therapies_table(tables)
            for n in sources:
                # CLI()._delete_data(n, dynamodb)
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
                    # TODO: Fix so that self._delete_data(n) works
                    # CLI()._delete_data(n, dynamodb)
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
            print(f"Deleted table: {table}")

    def _delete_data(self, source, dynamodb, *args, **kwargs):
        click.echo(f"Start deleting the {source} source.")
        src_names = [src.value for src in SourceName.__members__.values()]
        lower_src_names = [src.lower() for src in src_names]   # noqa F841
        # TODO: Delete therapies and meta data from source

        therapies_table = dynamodb.Table('Therapies')  # noqa F841
        metadata_table = dynamodb.Table('MetaData')  # noqa F841

        click.echo(f"Finished deleting the {source} source.")


if __name__ == '__main__':
    CLI().update_normalizer_db()
