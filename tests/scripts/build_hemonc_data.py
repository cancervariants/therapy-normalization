"""Construct hemonc test data."""
from pathlib import Path
import csv

from therapy.etl import HemOnc
from therapy.database import Database


TEST_IDS = ["65", "105", "151", "26"]

ho = HemOnc(Database())  # don't need to write any data
ho._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "hemonc"

with open(ho._src_files[0], "r") as f:
    concepts = list(csv.DictReader(f))
with open(ho._src_files[1], "r") as f:
    rels = list(csv.DictReader(f))
with open(ho._src_files[2], "r") as f:
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

with open(TEST_DATA_DIR / ho._src_files[0].name, "w") as f:
    writer = csv.DictWriter(f, concepts[0].keys())
    writer.writeheader()
    writer.writerows(test_concepts_rows)
with open(TEST_DATA_DIR / ho._src_files[1].name, "w") as f:
    writer = csv.DictWriter(f, rels[0].keys())
    writer.writeheader()
    writer.writerows(test_rels_rows)
with open(TEST_DATA_DIR / ho._src_files[2].name, "w") as f:
    writer = csv.DictWriter(f, syns[0].keys())
    writer.writeheader()
    writer.writerows(test_syn_rows)
