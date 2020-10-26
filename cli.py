"""This module provides a CLI util to make updates to normalizer database."""
import click
from therapy.etl import ChEMBL, Wikidata
from therapy.database import Base, engine, SessionLocal
from therapy import database, models, schemas  # noqa: F401
from therapy.models import Therapy
from sqlalchemy import event
from therapy.schemas import SourceName
from timeit import default_timer as timer


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

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(engine, rec):
            cursor = engine.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        engine.connect()
        Base.metadata.create_all(bind=engine)
        session = SessionLocal()

        sources = {
            'chembl': ChEMBL,
            'wikidata': Wikidata
        }

        if all:
            for n in sources:
                CLI()._delete_data(session, n)
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
                    CLI()._delete_data(session, n)
                    click.echo(f"Loading {n}...")
                    start = timer()
                    sources[n]()
                    end = timer()
                    click.echo(f"Loaded {n} in {end - start} seconds.")
                else:
                    raise Exception("Not a normalizer source.")
        session.close()

    def _delete_data(self, session, source, *args, **kwargs):
        click.echo(f"Start deleting the {source} source.")
        src_names = [src.value for src in SourceName.__members__.values()]
        lower_src_names = [src.lower() for src in src_names]
        delete_therapies = Therapy.__table__.delete().where(
            Therapy.src_name == src_names[lower_src_names.index(source)])
        session.execute(delete_therapies)
        session.commit()
        click.echo(f"Finished deleting the {source} source.")


if __name__ == '__main__':
    CLI().update_normalizer_db()
