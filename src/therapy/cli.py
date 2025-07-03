"""Provides a CLI util to make updates to normalizer database."""

import logging

import click
from disease.database import create_db as create_disease_db
from disease.etl.update import update_all_sources as update_disease_sources

from therapy import __version__
from therapy.config import config
from therapy.database import create_db
from therapy.etl.update import update_all_sources, update_normalized, update_source
from therapy.log import configure_logs
from therapy.schemas import SourceName

_logger = logging.getLogger(__name__)

URL_DESCRIPTION = 'URL endpoint for the application database. Must be a URL to a local DynamoDB server (e.g. "http://localhost:8001").'
SILENT_MODE_DESCRIPTION = "Suppress output to console."


def _initialize_app() -> None:
    if config.debug:
        configure_logs(log_level=logging.DEBUG)
    else:
        configure_logs()


@click.group()
@click.version_option(__version__)
def cli() -> None:
    """Manage Thera-Py data."""


@cli.command()
@click.option("--db_url", help=URL_DESCRIPTION)
@click.option("--silent", is_flag=True, default=False, help=SILENT_MODE_DESCRIPTION)
def check_db(db_url: str, silent: bool) -> None:
    """Perform basic checks on DB health and population. Exits with status code 1
    if DB schema is uninitialized or if critical tables appear to be empty.

        $ thera-py check-db
        $ echo $?
        1  # indicates failure

    This command is equivalent to the combination of the database classes'
    ``check_schema_initialized()`` and ``check_tables_populated()`` methods:

    >>> from therapy.database import create_db
    >>> db = create_db()
    >>> db.check_schema_initialized() and db.check_tables_populated()
    True  # DB passes checks

    \f
    :param db_url: URL to normalizer database
    :param silent: if true, suppress console output
    """  # noqa: D301
    _initialize_app()
    db = create_db(db_url, aws_instance=False)
    if not db.check_schema_initialized():
        if not silent:
            click.echo("Health check failed: DB schema uninitialized.")
        click.get_current_context().exit(1)

    if not db.check_tables_populated():
        if not silent:
            click.echo("Health check failed: DB is incompletely populated.")
        click.get_current_context().exit(1)

    msg = "DB health check successful: tables appear complete."
    if not silent:
        click.echo(msg)
    _logger.info(msg)


def _ensure_diseases_updated(from_local: bool) -> None:
    """Check disease DB. If it appears unpopulate, run a full update.

    :param from_local: if True, update from most recent locally available data
    """
    disease_db = create_disease_db()
    if (
        not disease_db.check_schema_initialized()
        or not disease_db.check_tables_populated()
    ):
        update_disease_sources(disease_db, use_existing=from_local, silent=True)


@cli.command()
@click.argument("sources", nargs=-1)
@click.option("--all", "all_", is_flag=True, help="Update records for all sources.")
@click.option("--normalize", is_flag=True, help="Create normalized records.")
@click.option("--db_url", help=URL_DESCRIPTION)
@click.option("--aws_instance", is_flag=True, help="Use cloud DynamodDB instance.")
@click.option(
    "--use_existing",
    is_flag=True,
    default=False,
    help="Use most recent locally-available source data instead of fetching latest version",
)
@click.option("--silent", is_flag=True, default=False, help=SILENT_MODE_DESCRIPTION)
def update(
    sources: tuple[str, ...],
    aws_instance: bool,
    db_url: str,
    all_: bool,
    normalize: bool,
    use_existing: bool,
    silent: bool,
) -> None:
    """Update provided normalizer SOURCES in the therapy database.

    Valid SOURCES are "ChEMBL", "ChemIdPlus", "DrugBank", "DrugsAtFDA",
    "GuideToPharmacology", "HemOnc", "NCIt", "RxNorm", and "Wikidata" (case is irrelevant).

    SOURCES are optional, but if not provided, either --all or --normalize must be used.

    For example, the following command will update NCIt and ChEMBL source records:

        $ thera-py update ncit chembl

    To completely reload all source records and construct normalized concepts, use the
    --all and --normalize options:

        $ thera-py update --all --normalize

    Thera-Py will fetch the latest available data from all sources if local
    data is out-of-date. To suppress this and force usage of local files only, use the
    --use_existing flag:

        $ thera-py update --all --use_existing

    \f
    :param sources: tuple of raw names of sources to update
    :param aws_instance: if true, use cloud instance
    :param db_url: URI pointing to database
    :param all_: if True, update all sources (ignore ``sources``)
    :param normalize: if True, update normalized records
    :param use_existing: if True, use most recent local data instead of fetching latest version
    :param silent: if True, suppress console output
    """  # noqa: D301
    _initialize_app()
    if len(sources) == 0 and (not all_) and (not normalize):
        click.echo(
            "Error: must provide SOURCES or at least one of --all, --normalize\n"
        )
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        ctx.exit(1)

    db = create_db(db_url, aws_instance)

    processed_ids = None
    if all_:
        _ensure_diseases_updated(use_existing)
        processed_ids = update_all_sources(db, use_existing, silent=silent)
    elif sources:
        parsed_sources = set()
        failed_source_names = []
        for source in sources:
            try:
                parsed_sources.add(SourceName[source.upper()])
            except KeyError:
                failed_source_names.append(source)
        if len(failed_source_names) != 0:
            click.echo(f"Error: unrecognized sources: {failed_source_names}")
            click.echo(f"Valid source options are {list(SourceName)}")
            click.get_current_context().exit(1)

        if SourceName.CHEMBL in parsed_sources or SourceName.HEMONC in parsed_sources:
            _ensure_diseases_updated(use_existing)
        working_processed_ids = set()
        for source_name in parsed_sources:
            working_processed_ids |= update_source(
                source_name, db, use_existing=use_existing, silent=silent
            )
        if len(sources) == len(SourceName):
            processed_ids = working_processed_ids

    if normalize:
        update_normalized(db, processed_ids, silent=silent)


if __name__ == "__main__":
    cli()
