"""This module creates the database."""
import boto3
from os import environ
from botocore.exceptions import ClientError
from typing import Optional, Dict
import logging
from boto3.dynamodb.conditions import Key

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class Database:
    """The database class."""

    def __init__(self, db_url='', region_name='us-east-2'):
        """Initialize Database class."""
        if db_url:
            self.ddb = boto3.resource('dynamodb', region_name=region_name,
                                      endpoint_url=db_url)
            self.ddb_client = boto3.client('dynamodb',
                                           region_name=region_name,
                                           endpoint_url=db_url)
        elif 'THERAPY_NORM_DB_URL' in environ.keys():
            db_url = environ['THERAPY_NORM_DB_URL']
            self.ddb = boto3.resource('dynamodb', region_name=region_name,
                                      endpoint_url=db_url)
            self.ddb_client = boto3.client('dynamodb',
                                           region_name=region_name,
                                           endpoint_url=db_url)
        else:
            self.ddb = boto3.resource('dynamodb', region_name=region_name)
            self.ddb_client = boto3.client('dynamodb',
                                           region_name=region_name)
        if db_url or 'THERAPY_NORM_DB_URL' in environ.keys():
            existing_tables = self.ddb_client.list_tables()['TableNames']
            self.create_therapies_table(existing_tables)
            self.create_meta_data_table(existing_tables)

        self.therapies = self.ddb.Table('therapy_concepts')
        self.metadata = self.ddb.Table('therapy_metadata')
        self.cached_sources = {}

    def create_therapies_table(self, existing_tables):
        """Create Therapies table if not exists."""
        table_name = 'therapy_concepts'
        if table_name not in existing_tables:
            self.ddb.create_table(
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

    def create_meta_data_table(self, existing_tables):
        """Create MetaData table if not exists."""
        table_name = 'therapy_metadata'
        if table_name not in existing_tables:
            self.ddb.create_table(
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
        except KeyError:
            return None

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
