"""Build DrugBank test data."""
import csv
from pathlib import Path

from therapy.database import Database
from therapy.etl import DrugBank

db = DrugBank(Database())
db._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
outfile_path = TEST_DATA_DIR / db._src_file.name

with open(db._src_file, "r") as f:
    rows = list(csv.DictReader(f))

concept_ids = [
    "DB06145",
    "DB01143",
    "DB01174",
    "DB12117",
    "DB00515",
    "DB00522",
    "DB14257",
]

write_rows = []
for row in rows:
    if row["DrugBank ID"] in concept_ids:
        write_rows.append(row)

with open(outfile_path, "w") as f:
    writer = csv.DictWriter(f, write_rows[0].keys())
    writer.writeheader()
    writer.writerows(write_rows)
