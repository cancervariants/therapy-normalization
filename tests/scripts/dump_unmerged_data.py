"""Import all test data to a fresh DDB, then dump to file.

Use for providing test data for checking normalized group generation.
"""
import json
from pathlib import Path
import sys
import os

import click

from therapy.cli import CLI
from therapy.database import Database, AWS_ENV_VAR_NAME
from therapy.etl import (
    ChEMBL,
    ChemIDplus,
    DrugBank,
    DrugsAtFDA,
    GuideToPHARMACOLOGY,
    HemOnc,
    NCIt,
    RxNorm,
    Wikidata,
)
from therapy.schemas import SourceName


if (
    AWS_ENV_VAR_NAME in os.environ
    or "THERAPY_TEST" not in os.environ  # noqa: W503
    and not click.confirm("Okay to wipe DDB?")  # noqa: W503
):
    sys.exit()


db = Database()
TEST_DATA_DIRECTORY = Path(__file__).resolve().parents[1] / "data"
with open(TEST_DATA_DIRECTORY / "disease_normalization.json", "r") as f:
    disease_data = json.load(f)

CLI()._delete_normalized_data(db)
for src_name in SourceName.__members__.values():
    CLI()._delete_data(src_name, db)


for EtlClass in (
    ChEMBL,
    ChemIDplus,
    DrugBank,
    DrugsAtFDA,
    GuideToPHARMACOLOGY,
    HemOnc,
    NCIt,
    RxNorm,
    Wikidata,
):
    test_class = EtlClass(db, TEST_DATA_DIRECTORY)
    test_class._normalize_disease = lambda q: disease_data.get(q.lower())  # type: ignore  # noqa: E501
    test_class.perform_etl(use_existing=True)
    test_class.database.flush_batch()

last_evaluated_key = None
items_list = []
while True:
    if last_evaluated_key:
        response = db.therapies.scan(ExclusiveStartKey=last_evaluated_key)
    else:
        response = db.therapies.scan()

    last_evaluated_key = response.get("LastEvaluatedKey")
    records = response["Items"]
    items_list += records

    if not last_evaluated_key:
        break

with open(TEST_DATA_DIRECTORY / "therapies.json", "w") as f:
    json.dump({"items": items_list}, f)

metadata = db.metadata.scan()["Items"]
with open(TEST_DATA_DIRECTORY / "metadata.json", "w") as f:
    json.dump({"items": metadata}, f)
