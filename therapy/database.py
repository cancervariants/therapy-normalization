"""This module creates the database."""
import boto3
import json
from therapy import PROJECT_ROOT
# from boto3.dynamodb.conditions import Key


class Database:
    """The database class."""

    def __init__(self, *args, **kwargs):
        """Initialize Database class."""
        self.db = boto3.resource('dynamodb',
                                 endpoint_url="http://localhost:8000")
        self.db_client = boto3.client('dynamodb',
                                      endpoint_url="http://localhost:8000")
        # existing_tables = self.db_client.list_tables()['TableNames']  # noqa
        # self.create_therapies_table(existing_tables)
        # self.create_meta_data_table(existing_tables)
        # self.load_chembl_data(dynamodb, dynamodb_client)
        # self.query(dynamodb)

    # def query(self, dynamodb):
    #     """Make a query."""
    #     table = dynamodb.Table('Therapies')
    #     try:
    #         response = table.query(
    #             KeyConditionExpression=Key(
    #                 'label_and_type').eq('chembl:chembl25##identity')
    #         )
    #         return response['Items']
    #     except ValueError:
    #         print("Not a valid query.")

    def load_chembl_data(self, dynamodb):
        """Load ChEMBL data into DynamoDB."""
        table = dynamodb.Table('Therapies')
        with open(f"{PROJECT_ROOT}/data/chembl/chembl.json") as f:
            chembl_data = json.load(f)

            with table.batch_writer() as batch:

                for data in chembl_data:
                    batch.put_item(Item=data)

    def create_therapies_table(self, existing_tables):
        """Create Therapies table if not exists."""
        table_name = 'Therapies'
        if table_name not in existing_tables:
            self.db.create_table(
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

    def create_meta_data_table(self, existing_tables):
        """Create MetaData table if not exists."""
        table_name = 'Metadata'
        if table_name not in existing_tables:
            self.db.create_table(
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


# Database()
DB = Database()
