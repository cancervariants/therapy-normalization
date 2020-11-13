"""This module creates the database."""
import boto3
from boto3.dynamodb.conditions import Key

DYNAMODB = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
DYNAMODBCLIENT = \
    boto3.client('dynamodb', endpoint_url="http://localhost:8000")
THERAPIES_TABLE = DYNAMODB.Table('Therapies')
METADATA_TABLE = DYNAMODB.Table('MetaData')


class Database:
    """The database class."""

    def __init__(self):
        """Initialize Database class."""
        existing_tables = DYNAMODBCLIENT.list_tables()['TableNames']
        self.create_therapies_table(DYNAMODB, existing_tables)
        self.create_meta_data_table(DYNAMODB, existing_tables)
        # self.query(dynamodb)

    def query(self, dynamodb):
        """Make a query."""
        table = dynamodb.Table('Therapies')
        try:
            response = table.query(
                KeyConditionExpression=Key(
                    'label_and_type').eq('chembl:chembl25##identity')
            )
            return response['Items']
        except ValueError:
            print("Not a valid query.")

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

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )

    def create_meta_data_table(self, dynamodb, existing_tables):
        """Create MetaData table if not exists."""
        table_name = 'MetaData'
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
