"""This module creates the database."""
import boto3
from os import environ
import logging
from typing import List
from botocore.exceptions import ClientError
from therapy.schemas import Drug

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class RecordNotFoundError(Exception):
    """Exception to handle record retrieval failure"""

    def __init__(self, message: str):
        """
        Create new instance

        :param str message: string describing context or detail of error
        """
        super().__init__(message)


class Database:
    """The database class."""

    def __init__(self, db_url: str = '', region_name: str = 'us-east-2'):
        """
        Initialize Database class.

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

        if db_url or 'THERAPY_NORM_DB_URL' in environ.keys():
            existing_tables = self.dynamodb_client.list_tables()['TableNames']
            self.create_therapies_table(existing_tables)
            self.create_meta_data_table(existing_tables)

        self.therapies = self.dynamodb.Table('therapy_concepts')
        self.metadata = self.dynamodb.Table('therapy_metadata')
        self.cached_sources = {}

    def create_therapies_table(self, existing_tables: List):
        """
        Create Therapies table if it doesn't already exist.

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
        """
        Create MetaData table if not exists.

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

    def get_record_by_id(self, concept_id: str) -> Drug:
        """
        Fetch record corresponding to provided concept ID

        :param str concept_id: concept ID for therapy record

        :return: complete therapy record
        :rtype: therapy.schemas.Drug

        :raises RecordNotFoundError: if no record exists for given cocnept ID
        """
        try:
            match = self.therapies.get_item(Key={
                'label_and_type': f'{concept_id.lower()}##identity',
                'concept_id': concept_id
            })
            item = match['Item']
            return Drug(**item)
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
        except KeyError:
            raise RecordNotFoundError
        return match
