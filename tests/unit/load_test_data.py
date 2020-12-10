"""Load test data into DynamoDB"""
import json
from therapy import PROJECT_ROOT
from therapy.database import Database

db = Database()

with open(f'{PROJECT_ROOT}/tests/unit/data/therapies', 'r') as f:
    therapies = json.load(f)
    with db.therapies.batch_writer() as batch:
        for therapy in therapies:
            batch.put_item(Item=therapy)
    f.close()

with open(f'{PROJECT_ROOT}/tests/unit/data/metadata', 'r') as f:
    metadata = json.load(f)
    with db.metadata.batch_writer() as batch:
        for m in metadata:
            batch.put_item(Item=m)
    f.close()
