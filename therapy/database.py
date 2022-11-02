"""This module creates the database."""
from os import environ
from typing import Optional, Dict, List, Any
from enum import Enum
import logging
import sys
import atexit

import click
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from therapy import PREFIX_LOOKUP

logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


# can be set to either `Dev`, `Staging`, or `Prod`
# ONLY set when wanting to access aws instance
AWS_ENV_VAR_NAME = "THERAPY_NORM_ENV"

# Set to "true" if want to skip db confirmation check. Should ONLY be used for
# deployment needs
SKIP_AWS_DB_ENV_NAME = "SKIP_AWS_CONFIRMATION"


class AwsEnvName(str, Enum):
    """AWS environment name that is being used"""

    DEVELOPMENT = "Dev"
    STAGING = "Staging"
    PRODUCTION = "Prod"


VALID_AWS_ENV_NAMES = {v.value for v in AwsEnvName.__members__.values()}


def confirm_aws_db_use(env_name: str) -> None:
    """Check to ensure that AWS instance should actually be used."""
    if click.confirm(f"Are you sure you want to use the AWS {env_name} database?",
                     default=False):
        click.echo(f"***THERAPY AWS {env_name.upper()} DATABASE IN USE***")
    else:
        click.echo("Exiting.")
        sys.exit()


class Database:
    """The database class."""

    def __init__(self, db_url: str = "", region_name: str = "us-east-2"):
        """Initialize Database class instance.

        :param str db_url: database endpoint URL to connect to
        :param str region_name: AWS region name to use
        """
        therapy_concepts_table = "therapy_concepts"  # default
        therapy_metadata_table = "therapy_metadata"  # default

        if AWS_ENV_VAR_NAME in environ:
            aws_env = environ[AWS_ENV_VAR_NAME]
            assert aws_env in VALID_AWS_ENV_NAMES, f"{AWS_ENV_VAR_NAME} must be one of {VALID_AWS_ENV_NAMES}"  # noqa: E501

            skip_confirmation = environ.get(SKIP_AWS_DB_ENV_NAME)
            if (not skip_confirmation) or (skip_confirmation and skip_confirmation != "true"):  # noqa: E501
                confirm_aws_db_use(environ[AWS_ENV_VAR_NAME])

            boto_params = {
                "region_name": region_name
            }

            if aws_env == AwsEnvName.DEVELOPMENT:
                therapy_concepts_table = "therapy_concepts_nonprod"
                therapy_metadata_table = "therapy_metadata_nonprod"
        else:
            if db_url:
                self.endpoint_url = db_url
            elif "THERAPY_NORM_DB_URL" in environ:
                self.endpoint_url = environ["THERAPY_NORM_DB_URL"]
            else:
                self.endpoint_url = "http://localhost:8000"
            click.echo(f"***Using Therapy Database Endpoint: "
                       f"{self.endpoint_url}***")
            environ["DISEASE_NORM_DB_URL"] = self.endpoint_url

            boto_params = {
                "region_name": region_name,
                "endpoint_url": self.endpoint_url
            }

        self.dynamodb = boto3.resource("dynamodb", **boto_params)
        self.dynamodb_client = boto3.client("dynamodb", **boto_params)

        # Only create tables for local instance
        if not {AWS_ENV_VAR_NAME} & set(environ):
            existing_tables = self.dynamodb_client.list_tables()["TableNames"]
            self.create_therapies_table(existing_tables)
            self.create_meta_data_table(existing_tables)

        self.therapies = self.dynamodb.Table(therapy_concepts_table)
        self.metadata = self.dynamodb.Table(therapy_metadata_table)
        self.batch = self.therapies.batch_writer()
        self.cached_sources: Dict[str, Any] = {}
        atexit.register(self.flush_batch)

    def create_therapies_table(self, existing_tables: List[str]) -> None:
        """Create Therapies table if it doesn"t already exist.

        :param List[str] existing_tables: list of existing table names
        """
        table_name = "therapy_concepts"
        if table_name not in existing_tables:
            self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        "AttributeName": "label_and_type",
                        "KeyType": "HASH"  # Partition key
                    },
                    {
                        "AttributeName": "concept_id",
                        "KeyType": "RANGE"  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        "AttributeName": "label_and_type",
                        "AttributeType": "S"
                    },
                    {
                        "AttributeName": "concept_id",
                        "AttributeType": "S"
                    },
                    {
                        "AttributeName": "src_name",
                        "AttributeType": "S"
                    },
                    {
                        "AttributeName": "item_type",
                        "AttributeType": "S"
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "src_index",
                        "KeySchema": [
                            {
                                "AttributeName": "src_name",
                                "KeyType": "HASH"
                            }
                        ],
                        "Projection": {
                            "ProjectionType": "KEYS_ONLY"
                        },
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                            "WriteCapacityUnits": 10
                        }
                    },
                    {
                        "IndexName": "item_type_index",
                        "KeySchema": [
                            {
                                "AttributeName": "item_type",
                                "KeyType": "HASH"
                            }
                        ],
                        "Projection": {
                            "ProjectionType": "KEYS_ONLY"
                        },
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 10,
                            "WriteCapacityUnits": 10
                        }
                    }
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10
                }
            )

    def create_meta_data_table(self, existing_tables: List[str]) -> None:
        """Create MetaData table if not exists.

        :param List existing_tables: list of existing table names
        """
        table_name = "therapy_metadata"
        if table_name not in existing_tables:
            self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        "AttributeName": "src_name",
                        "KeyType": "HASH"  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        "AttributeName": "src_name",
                        "AttributeType": "S"
                    },
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10
                }
            )

    def get_record_by_id(self, concept_id: str, case_sensitive: bool = True,
                         merge: bool = False) -> Optional[Dict]:
        """Fetch record corresponding to provided concept ID

        :param str concept_id: concept ID for therapy record
        :param bool case_sensitive: if true, performs exact lookup, which is more
            efficient. Otherwise, performs filter operation, which doesn"t require
            correct casing.
        :param bool merge: if true, look for merged concept
        :return: complete therapy record, if match is found; None otherwise
        """
        try:
            if merge:
                pk = f"{concept_id.lower()}##merger"
            else:
                pk = f"{concept_id.lower()}##identity"
            if case_sensitive:
                match = self.therapies.get_item(Key={
                    "label_and_type": pk,
                    "concept_id": concept_id
                })
                return match["Item"]
            else:
                exp = Key("label_and_type").eq(pk)
                response = self.therapies.query(KeyConditionExpression=exp)
                return response["Items"][0]
        except ClientError as e:
            logger.error(f"boto3 client error on get_records_by_id for "
                         f"search term {concept_id}: "
                         f"{e.response['Error']['Message']}")
            return None
        except (KeyError, IndexError):  # record doesn"t exist
            return None

    def get_records_by_type(self, query: str,
                            match_type: str) -> List[Dict]:
        """Retrieve records for given query and match type.

        :param query: string to match against (should already be lower-case)
        :param str match_type: type of match to look for. Should be one of {"alias",
            "trade_name", "label", "xref", "associated_with", "rx_brand"} -- use
            Database.get_record_by_id() for concept ID lookup
        :return: list of matching records. Empty if lookup fails.
        """
        pk = f"{query}##{match_type.lower()}"
        filter_exp = Key("label_and_type").eq(pk)
        try:
            matches = self.therapies.query(KeyConditionExpression=filter_exp)
            return matches.get("Items", None)
        except ClientError as e:
            logger.error(f"boto3 client error on get_records_by_type for "
                         f"search term {query}: "
                         f"{e.response['Error']['Message']}")
            return []

    def get_ids_for_merge(self) -> List[str]:
        """Retrieve concept IDs for use in generating normalized records.

        :return: List of concept IDs as strings.
        """
        last_evaluated_key = None
        concept_ids = []
        params = {
            "ProjectionExpression": "concept_id,item_type",
        }
        while True:
            if last_evaluated_key:
                response = self.therapies.scan(
                    ExclusiveStartKey=last_evaluated_key, **params
                )
            else:
                response = self.therapies.scan(**params)
            records = response["Items"]
            for record in records:
                if record["item_type"] == "identity":
                    concept_id = record["concept_id"]
                    concept_ids.append(concept_id)
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break
        return concept_ids

    def add_record(self, record: Dict, record_type: str = "identity") -> None:
        """Add new record to database.

        :param Dict record: record to upload
        :param str record_type: type of record, one of {"identity", "merger"}
        """
        id_prefix = record["concept_id"].split(":")[0].lower()
        record["src_name"] = PREFIX_LOOKUP[id_prefix]
        label_and_type = f"{record['concept_id'].lower()}##{record_type}"
        record["label_and_type"] = label_and_type
        record["item_type"] = record_type
        try:
            self.batch.put_item(Item=record)
        except ClientError as e:
            if (len((record["label_and_type"]).encode("utf-8")) >= 2048) or \
                    (len((record["concept_id"]).encode("utf-8")) >= 1024):
                logger.info(f"{record['concept_id']}: An error occurred "
                            "(ValidationException) when calling the BatchWriteItem "
                            "operation: Hash primary key values must be under 2048 "
                            "bytes, and range primary key values must be under 1024 "
                            "bytes.")
            else:
                logger.error("boto3 client error on add_record for "
                             f"{record['concept_id']}: "
                             f"{e.response['Error']['Message']}")

    def add_ref_record(self, term: str, concept_id: str, ref_type: str) -> None:
        """Add auxiliary/reference record to database.

        :param str term: referent term
        :param str concept_id: concept ID to refer to
        :param str ref_type: one of {"alias", "label", "trade_name",
            "xref", "associated_with", "rx_brand"}
        """
        label_and_type = f"{term.lower()}##{ref_type}"
        src_name = PREFIX_LOOKUP[concept_id.split(":")[0].lower()]
        record = {
            "label_and_type": label_and_type,
            "concept_id": concept_id.lower(),
            "src_name": src_name,
            "item_type": ref_type,
        }
        try:
            self.batch.put_item(Item=record)
        except ClientError as e:
            if (len((record["label_and_type"]).encode("utf-8")) >= 2048) or \
                    (len((record["concept_id"]).encode("utf-8")) >= 1024):
                logger.info(f"{record['concept_id']}: An error occurred "
                            "(ValidationException) when calling the "
                            "BatchWriteItem operation: Hash primary key "
                            "values must be under 2048 bytes, and range "
                            "primary key values must be under 1024 "
                            "bytes.")
            else:
                logger.error(f"boto3 client error adding reference {term} for "
                             f"{concept_id} with match type {ref_type}: "
                             f"{e.response['Error']['Message']}")

    def update_record(self, concept_id: str, field: str, new_value: Any,  # noqa: ANN401
                      item_type: str = "identity") -> None:
        """Update the field of an individual record to a new value.

        :param str concept_id: record to update
        :param str field: name of field to update
        :param Any new_value: new value
        :param str item_type: record type, one of {"identity", "merger"}
        """
        key = {
            "label_and_type": f"{concept_id.lower()}##{item_type}",
            "concept_id": concept_id
        }
        update_expression = f"set {field}=:r"
        update_values = {":r": new_value}
        try:
            self.therapies.update_item(Key=key,
                                       UpdateExpression=update_expression,
                                       ExpressionAttributeValues=update_values)
        except ClientError as e:
            logger.error(f"boto3 client error in `database.update_record()`: "
                         f"{e.response['Error']['Message']}")

    def flush_batch(self) -> None:
        """Flush internal batch_writer."""
        self.batch.__exit__(*sys.exc_info())
        self.batch = self.therapies.batch_writer()
