"""This module provides a CLI util to make updates to normalizer database."""
import click
from therapy.etl import ChEMBL, Wikidata
from therapy.database import Base
from therapy import database, models, schemas  # noqa: F401

normalizers_dict = {
    'chembl': ChEMBL,
    'wikidata': Wikidata
}


class CLI:
    """Class for updating the normalizer database via Click"""

    @click.command()
    @click.option(
        '--all',
        is_flag=True,
        help='Update all normalizer databases'
    )
    @click.option(
        '--normalizer',
        help="The normalizer(s) you wish to update separated by spaces."
    )
    def update_normalizer_db(normalizer, all):
        """Update select normalizer(s) in the database."""
        if all:
            for n in normalizers_dict:
                CLI()._delete_data(n)
                normalizers_dict[n]()
                click.echo(f"Finished updating the {n} source")
            click.echo('Updated all of the normalizer sources.')
        else:
            normalizers = normalizer.lower().split()
            for n in normalizers:
                if n in normalizers_dict:
                    # TODO: Fix so that self._delete_data(n) works
                    CLI()._delete_data(n)
                    normalizers_dict[n]()
                    click.echo(f"Finished updating the {n} source")
                else:
                    raise Exception("Not a normalizer.")

    def _delete_data(self, source, *args, **kwargs):
        # TODO: Fix so that delete cascade works and use SQLAlchemy commands
        Base.metadata.create_all(bind=database.engine)

        delete_therapies = f"""
            DELETE FROM therapies
            WHERE LOWER(concept_id) LIKE LOWER('{source}%');
        """
        database.engine.execute(delete_therapies)

        delete_aliases = f"""
            DELETE FROM aliases
            WHERE LOWER(concept_id) LIKE LOWER('{source}%');
        """
        database.engine.execute(delete_aliases)

        delete_other_identifiers = f"""
            DELETE FROM other_identifiers
            WHERE LOWER(concept_id) LIKE LOWER('{source}%');
        """
        database.engine.execute(delete_other_identifiers)


if __name__ == '__main__':
    CLI().update_normalizer_db()
