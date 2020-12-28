"""This module creates the database."""
import boto3
from os import environ
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from therapy.schemas import Drug

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class RecordNotFoundError(Exception):
    """Exception to handle record retrieval failure"""

    def __init__(self, message):
        """Create new instance

        :param str message: string describing context or detail of error
        """
        super().__init__(message)


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

    def get_record_by_id(self, concept_id: str) -> Drug:
        """Fetch record corresponding to provided concept ID

        :param str concept_id: concept ID for therapy record

        :return: complete therapy record
        :rtype: therapy.schemas.Drug

        :raises RecordNotFoundError: if no record exists for given cocnept ID
        """
        try:
            pk = f'{concept_id.lower()}##identity'
            filter_exp = Key('label_and_type').eq(pk)
            result = self.therapies.query(KeyConditionExpression=filter_exp)
            match = result['Items'][0]
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
        except IndexError:
            raise RecordNotFoundError
        return match

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
