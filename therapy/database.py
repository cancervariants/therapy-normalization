"""This module creates the database."""
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from os import environ
from typing import Optional, Dict, List, Any
import logging

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Database:
    """The database class."""

    def __init__(self, db_url: str = '', region_name: str = 'us-east-2'):
        """Initialize Database class.

        :param str db_url: database endpoint URL to connect to
        :param str region_name: AWS region name to use
        """
        if db_url:
            boto_params = {
                'region_name': region_name,
                'endpoint_url': db_url
            }
        elif 'THERAPY_NORM_DB_URL' in environ.keys():
            boto_params = {
                'region_name': region_name,
                'endpoint_url': environ['THERAPY_NORM_DB_URL']
            }
        else:
            boto_params = {
                'region_name': region_name
            }
        self.dynamodb = boto3.resource('dynamodb', **boto_params)
        self.dynamodb_client = boto3.client('dynamodb', **boto_params)

        # create tables if nonexistant if not connecting to remote database
        if db_url or 'THERAPY_NORM_DB_URL' in environ.keys():
            existing_tables = self.dynamodb_client.list_tables()['TableNames']
            self.create_therapies_table(existing_tables)
            self.create_meta_data_table(existing_tables)

        self.therapies = self.dynamodb.Table('therapy_concepts')
        self.metadata = self.dynamodb.Table('therapy_metadata')
        self.batch = self.therapies.batch_writer()
        self.cached_sources = {}

    def create_therapies_table(self, existing_tables: List):
        """Create Therapies table if it doesn't already exist.

        :param List existing_tables: list of existing table names
        """
        table_name = 'therapy_concepts'
        if table_name not in existing_tables:
            self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'label_and_type',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'concept_id',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'label_and_type',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'concept_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'src_name',
                        'AttributeType': 'S'
                    }

                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'src_index',
                        'KeySchema': [
                            {
                                'AttributeName': 'src_name',
                                'KeyType': 'HASH'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'KEYS_ONLY'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 10,
                            'WriteCapacityUnits': 10
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )

    def create_meta_data_table(self, existing_tables: List):
        """Create MetaData table if not exists.

        :param List existing_tables: list of existing table names
        """
        table_name = 'therapy_metadata'
        if table_name not in existing_tables:
            self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'src_name',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'src_name',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )

    def get_record_by_id(self, concept_id: str,
                         case_sensitive: bool = True) -> Optional[Dict]:
        """Fetch record corresponding to provided concept ID

        :param str concept_id: concept ID for therapy record
        :param bool case_sensitive: if true, performs exact lookup, which is
            more efficient. Otherwise, performs filter operation, which
            doesn't require correct casing.
        :return: complete therapy record, if match is found; None otherwise
        """
        try:
            pk = f'{concept_id.lower()}##identity'
            if case_sensitive:
                match = self.therapies.get_item(Key={
                    'label_and_type': pk,
                    'concept_id': concept_id
                })
                return match['Item']
            else:
                exp = Key('label_and_type').eq(pk)
                response = self.therapies.query(KeyConditionExpression=exp)
                return response['Items'][0]
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            return None
        except KeyError:  # record doesn't exist
            return None
        except IndexError:  # record doesn't exist
            return None

    def get_records_by_type(self, query: str,
                            match_type: str) -> List[Dict]:
        """Retrieve records for given query and match type.

        :param query: string to match against
        :param str match_type: type of match to look for. Should be one
            of "alias", "trade_name", or "label" (use get_record_by_id for
            concept ID lookup)
        :return: list of matching records. Empty if lookup fails.
        """
        pk = f'{query}##{match_type.lower()}'
        filter_exp = Key('label_and_type').eq(pk)
        try:
            matches = self.therapies.query(KeyConditionExpression=filter_exp)
            if 'Items' in matches.keys():
                return matches.get('Items', {None})
        except ClientError as e:
            logger(e.response['Error']['Message'])
            return []

    def get_merged_record(self, merge_ref) -> Optional[Dict]:
        """Fetch merged record from given reference.

        :param str merge_ref: key for merged record, formated as a string
            of grouped concept IDs separated by vertical bars, ending with
            `##merger`. Must be correctly-cased.
        :return: complete merged record if lookup successful, None otherwise
        """
        try:
            match = self.therapies.get_item(Key={
                'label_and_type': merge_ref,
                'concept_id': merge_ref
            })
            return match['Item']
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            return None
        except KeyError:
            return None

    def update_record(self, concept_id: str, field: str, value: Any):
        """Update the field of an individual record to a new value.

        :param str concept_id: record to update
        :param str field: name of field to update
        :parm str value: new value
        """
        key = {
            'label_and_type': f'{concept_id.lower()}##identity',
            'concept_id': concept_id
        }
        update = {
            'Value': {
                field: value
            }
        }
        try:
            self.therapies.update_item(Key=key, AttributeUpdates=update)
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
