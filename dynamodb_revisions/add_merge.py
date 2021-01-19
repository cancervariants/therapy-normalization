"""Add merged records and references to database."""
from therapy.database import Database
from therapy.etl.merge import Merge


def main():
    """Execute scan on DB to gather concept IDs and add merged concepts."""
    db = Database()
    concept_ids = set()

    done = False
    start_key = None
    args = dict()
    while not done:
        if start_key:
            args['ExclusiveStartKey'] = start_key
        response = db.therapies.scan(**args)
        items = response['Items']
        for item in items:
            if item['label_and_type'].endswith('##identity'):
                concept_ids.add(item['concept_id'])

        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    merge = Merge(db)

    merge.create_merged_concepts(concept_ids)


if __name__ == '__main__':
    main()
