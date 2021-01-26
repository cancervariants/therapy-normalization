"""Add merged records and references to database."""
from therapy.database import Database
from therapy.etl.merge import Merge
from timeit import default_timer as timer
from therapy.schemas import SourceName
from therapy import PROJECT_ROOT


def main():
    """Execute scan on DB to gather concept IDs and add merged concepts."""
    start = timer()
    db = Database(db_url='http://localhost:8000')

    concept_ids_path = PROJECT_ROOT / 'dynamodb_revisions' / 'concept_ids.txt'
    if concept_ids_path.exists():
        with open(concept_ids_path) as f:
            concept_ids = f.read().splitlines()
    else:
        print("Compiling concept IDs...")
        concept_ids = []
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
                    if 'src_name' not in item:
                        print(item)
                    if item['src_name'] != SourceName.CHEMBL.value and \
                            item['src_name'] != SourceName.DRUGBANK.value:
                        concept_ids.append(item['concept_id'])

            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        with open(concept_ids_path, 'w') as f:
            for concept_id in concept_ids:
                print(concept_id, file=f)

    print("Concept ID scan successful.")
    print(f"{len(concept_ids)} concept IDs gathered.")

    merge = Merge(db)

    merge.create_merged_concepts(concept_ids)
    end = timer()
    print(f"Merge generation successful, time: {end - start}")


if __name__ == '__main__':
    main()
