"""Test database"""
import os

import pytest


def test_tables_created(database):
    """Check that required tables are created."""
    existing_tables = database.list_tables()
    if database.__class__.__name__ == "PostgresDatabase":
        assert set(existing_tables) == {
            "therapy_associations",
            "therapy_labels",
            "therapy_aliases",
            "therapy_xrefs",
            "therapy_concepts",
            "therapy_merged",
            "therapy_rx_brand_ids",
            "therapy_sources",
            "therapy_trade_names",
        }
    else:
        assert "therapy_concepts" in existing_tables
        assert "therapy_metadata" in existing_tables


IS_DDB = not os.environ.get("THERAPY_NORM_DB_URL", "").lower().startswith("postgres")


@pytest.mark.skipif(not IS_DDB, reason="only applies to DynamoDB")
def test_item_type(database):  # noqa: F811
    """Check that objects are tagged with item_type attribute."""
    from boto3.dynamodb.conditions import Key

    filter_exp = Key("label_and_type").eq("chembl:chembl11359##identity")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "identity"

    filter_exp = Key("label_and_type").eq("interferon alfacon-1##label")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "label"

    filter_exp = Key("label_and_type").eq("unii:5x5hb3vz3z##associated_with")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "associated_with"

    filter_exp = Key("label_and_type").eq("drugbank:database00515##xref")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "xref"

    filter_exp = Key("label_and_type").eq("dichlorodiammineplatinum##alias")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "alias"

    filter_exp = Key("label_and_type").eq("rxcui:1041527##rx_brand")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "rx_brand"

    filter_exp = Key("label_and_type").eq("align##trade_name")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "trade_name"

    filter_exp = Key("label_and_type").eq("rxcui:9991##merger")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "merger"
