"""Other_id ref, update normalized data for production DynamoDB instance."""
from therapy.database import Database
from timeit import default_timer as timer
from boto3.dynamodb.conditions import Attr
from therapy.schemas import SourceName

db = Database()

normalized_srcs = {SourceName.WIKIDATA.value,
                   SourceName.RXNORM.value,
                   SourceName.NCIT.value,
                   SourceName.CHEMIDPLUS.value}


def perform_updates():
    """Update records:
    * Change merged record IDs from serialized concept IDs to main IDs. Also
      update references from identity records.
    * Add other_id refs for all records in `other_identifiers` field in
      identity record.
    """
    # update merge_refs and add other_id refs for normalized sources
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = db.therapies.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = db.therapies.scan()
        records = response.get('Items', [])

        for record in records:
            if record['label_and_type'].endswith('identity'):
                concept_id = record['concept_id']
                if record['src_name'] in normalized_srcs:
                    old_ref = record['merge_ref']
                    if '|' in old_ref:
                        new_ref = old_ref.split('|', 1)[0]
                        db.update_record(concept_id, 'merge_ref', new_ref)

                for other_id in record.get('other_identifiers', []):
                    db.add_ref_record(other_id, concept_id, 'other_id')

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    # update concept IDs for merged records
    last_evaluated_key = None
    # TODO double check that this filter will work correctly
    merge_filter = Attr('src_name').not_exists()
    while True:
        if last_evaluated_key:
            response = db.therapies.scan(ExclusiveStartKey=last_evaluated_key,
                                         FilterExpression=merge_filter)
        else:
            response = db.therapies.scan(FilterExpression=merge_filter)

        records = response.get('Items', [])

        for record in records:
            old_label_and_type = record['label_and_type']
            if old_label_and_type.endswith('merger'):
                old_concept_id = record['old_concept_id']
                db.batch.delete_item(Key={
                    'label_and_type': old_label_and_type,
                    'concept_id': old_concept_id,
                })

                new_concept_id = old_concept_id.split('|', 1)[0]
                record['concept_id'] = new_concept_id
                record['label_and_type'] = f"{new_concept_id.lower()}##merger"
                db.add_record(record, 'merger')

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break


if __name__ == '__main__':
    print("Perfoming updates...")
    start = timer()
    perform_updates()
    end = timer()
    print(f"Updated records in {end - start:.5f} seconds")
