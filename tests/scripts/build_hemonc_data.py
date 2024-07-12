"""Construct hemonc test data."""

import csv
from pathlib import Path

from therapy.database import create_db
from therapy.etl import HemOnc

TEST_IDS = ["65", "105", "151", "26", "66258", "48"]

ho = HemOnc(create_db())  # don't need to write any data
ho._extract_data(False)
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "hemonc"

with ho._data_files.concepts.open() as f:
    concepts = list(csv.DictReader(f))
with ho._data_files.rels.open() as f:
    rels = list(csv.DictReader(f))
with ho._data_files.synonyms.open() as f:
    syns = list(csv.DictReader(f))

concept_ids = set()
for row in concepts:
    if row["concept_code"] in TEST_IDS:
        concept_ids.add(row["concept_code"])

test_rels_rows = []
rel_ids = set()
for row in rels:
    if row["concept_code_1"] in concept_ids:
        test_rels_rows.append(row)
        rel_ids.add(row["concept_code_2"])

rel_ids |= concept_ids
test_concepts_rows = []
for row in concepts:
    if row["concept_code"] in rel_ids:
        test_concepts_rows.append(row)

test_syn_rows = []
for row in syns:
    if row["synonym_concept_code"] in concept_ids:
        test_syn_rows.append(row)

with (TEST_DATA_DIR / ho._data_files.concepts.name).open("w") as f:
    writer = csv.DictWriter(f, concepts[0].keys())
    writer.writeheader()
    writer.writerows(test_concepts_rows)
with (TEST_DATA_DIR / ho._data_files.rels.name).open("w") as f:
    writer = csv.DictWriter(f, rels[0].keys())
    writer.writeheader()
    writer.writerows(test_rels_rows)
with (TEST_DATA_DIR / ho._data_files.synonyms.name).open("w") as f:
    writer = csv.DictWriter(f, syns[0].keys())
    writer.writeheader()
    writer.writerows(test_syn_rows)
