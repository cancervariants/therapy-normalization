"""This module creates the database."""
import boto3

DYNAMODB = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
DYNAMODBCLIENT = \
    boto3.client('dynamodb', endpoint_url="http://localhost:8000")
THERAPIES_TABLE = DYNAMODB.Table('Therapies')
METADATA_TABLE = DYNAMODB.Table('Metadata')


class Database:
    """The database class."""

    def __init__(self, *args, **kwargs):
        """Initialize Database class."""
        existing_tables = DYNAMODBCLIENT.list_tables()['TableNames']
        self.create_therapies_table(DYNAMODB, existing_tables)
        self.create_meta_data_table(DYNAMODB, existing_tables)

    def create_therapies_table(self, dynamodb, existing_tables):
        """Create Therapies table if not exists."""
        table_name = 'Therapies'
        if table_name not in existing_tables:
            dynamodb.create_table(
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

    def create_meta_data_table(self, dynamodb, existing_tables):
        """Create MetaData table if not exists."""
        table_name = 'Metadata'
        if table_name not in existing_tables:
            dynamodb.create_table(
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
