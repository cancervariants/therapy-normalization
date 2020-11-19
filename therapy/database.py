"""This module creates the database."""
import boto3

DYNAMODB = boto3.resource('dynamodb', region_name='us-east-2')
DYNAMODBCLIENT = boto3.client('dynamodb', region_name='us-east-2')
THERAPIES_TABLE = DYNAMODB.Table('therapy_concepts')
METADATA_TABLE = DYNAMODB.Table('therapy_metadata')
cached_sources = dict()
