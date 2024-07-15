"""Test DynamoDB.

In the future, this should be filled in with different tests for alternate
implementations.
"""

from boto3.dynamodb.conditions import Key


def test_tables_created(database):
    """Check that therapy_concepts and therapy_metadata are created."""
    existing_tables = database.dynamodb_client.list_tables()["TableNames"]
    assert "therapy_normalizer" in existing_tables


def test_item_type(database):
    """Check that objects are tagged with item_type attribute."""
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

    filter_exp = Key("label_and_type").eq("drugbank:db00515##xref")
    item = database.therapies.query(KeyConditionExpression=filter_exp)["Items"][0]
    assert "item_type" in item
    assert item["item_type"] == "xref"

    filter_exp = Key("label_and_type").eq("spiramycin i##alias")
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
