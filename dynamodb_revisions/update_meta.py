"""Update source metadata in prod"""
from therapy.database import Database
from therapy.schemas import SourceName


def main():
    """Update DB with new source meta info."""
    db = Database()
    updates = {
        SourceName.CHEMBL.value: {
            "rdp_url": "http://reusabledata.org/chembl.html",
            "non_commercial": False,
            "share_alike": True,
            "attribution": True
        },
        SourceName.DRUGBANK.value: {
            "rdp_url": "http://reusabledata.org/drugbank.html",
            "non_commercial": True,
            "share_alike": False,
            "attribution": True
        },
        SourceName.NCIT.value: {
            "rdp_url": "http://reusabledata.org/ncit.html",
            "non_commercial": False,
            "share_alike": False,
            "attribution": True
        },
        SourceName.WIKIDATA.value: {
            "rdp_url": None,
            "non_commercial": False,
            "share_alike": False,
            "attribution": False
        }
    }

    for src_name in updates.keys():
        attrs = {k: {'Value': v} for k, v in updates[src_name].items()}
        db.metadata.update_item(
            Key={'src_name': src_name},
            AttributeUpdates=attrs
        )


if __name__ == '__main__':
    main()
