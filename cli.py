"""This module provides a CLI util to make updates to normalizer database."""
import click
from therapy.etl import ChEMBL, Wikidata
from therapy.database import Base, engine, SessionLocal
from therapy import database, models, schemas  # noqa: F401
from therapy.models import Therapy
from sqlalchemy import event

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
        help='Update all normalizer databases.'
    )
    @click.option(
        '--normalizer',
        help="The normalizer(s) you wish to update separated by spaces."
    )
    def update_normalizer_db(normalizer, all):
        """Update select normalizer(s) in the database."""

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(engine, rec):
            cursor = engine.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        engine.connect()

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
        Base.metadata.create_all(bind=engine)
        session = SessionLocal()

        session.query(Therapy).filter(Therapy.src_name.ilike(f"%{source}%")).\
            delete(synchronize_session=False)
        session.commit()


if __name__ == '__main__':
    CLI().update_normalizer_db()
