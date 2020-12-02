"""This module creates the database."""
import boto3


class Database:
    """The database class."""

    def __init__(self, db_url='', region_name='us-east-2',
                 create_tables=False):
        """Initialize Database class."""
        if db_url:
            self.ddb = boto3.resource('dynamodb', region_name=region_name,
                                      endpoint_url=db_url)
            self.ddb_client = boto3.resource('dynamodb',
                                             region_name=region_name,
                                             endpoint_url=db_url)
        else:
            self.ddb = boto3.resource('dynamodb', region_name=region_name)
            self.ddb_client = boto3.resource('dynamodb',
                                             region_name=region_name)
        if create_tables:
            existing_tables = self.ddb_client.list_tables()['TableNames']
            self.create_therapies_table(existing_tables)
            self.create_therapies_table(existing_tables)
        self.therapies = self.ddb.Table('therapy_concepts')
        self.metadata = self.ddb.Table('therapy_metadata')
        self.cached_sources = {}

    def create_therapies_table(self, existing_tables):
        """Create Therapies table if not exists."""
        table_name = 'Therapies'
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
        table_name = 'Metadata'
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
