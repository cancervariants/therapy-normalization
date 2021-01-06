"""Update source metadata in prod"""
import sys
from pathlib import Path
import click
from os import environ

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(f"{PROJECT_ROOT}")

from therapy.database import Database  # noqa: E402
from therapy.schemas import SourceName  # noqa: E402


def main():
    """Update DB with new source meta info."""
    db = Database()
    updates = {
        SourceName.CHEMBL.value: {
            "rdp_url": "http://reusabledata.org/chembl.html",
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": True,
                "attribution": True
            }
        },
        SourceName.DRUGBANK.value: {
            "rdp_url": "http://reusabledata.org/drugbank.html",
            "data_license_attributes": {
                "non_commercial": True,
                "share_alike": False,
                "attribution": True
            }
        },
        SourceName.NCIT.value: {
            "rdp_url": "http://reusabledata.org/ncit.html",
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": False,
                "attribution": True
            }
        },
        SourceName.WIKIDATA.value: {
            "rdp_url": None,
            "data_license_attributes": {
                "non_commercial": False,
                "share_alike": False,
                "attribution": False
            }
        }
    }

    for src_name in updates.keys():
        attrs = {k: {'Value': v} for k, v in updates[src_name].items()}
        db.metadata.update_item(
            Key={'src_name': src_name},
            AttributeUpdates=attrs
        )


if __name__ == '__main__':
    if 'THERAPY_NORM_DB_URL' not in environ.keys():
        if click.confirm("Are you sure you want to update"
                         " the production database?", default=False):
            click.echo("Updating production db...")
        else:
            click.echo("Exiting.")
            sys.exit()
    main()
