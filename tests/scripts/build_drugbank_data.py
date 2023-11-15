"""Build DrugBank test data."""
import csv
from pathlib import Path

from therapy.database import create_db
from therapy.etl import DrugBank

TEST_IDS = [
    "DB06145",
    "DB01143",
    "DB01174",
    "DB12117",
    "DB00515",
    "DB00522",
    "DB14257",
]

db = DrugBank(create_db())
db._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "drugbank"
outfile_path = TEST_DATA_DIR / db._src_file.name

with open(db._src_file, "r") as f:
    rows = list(csv.DictReader(f))

write_rows = []
for row in rows:
    if row["DrugBank ID"] in TEST_IDS:
        write_rows.append(row)

with open(outfile_path, "w") as f:
    writer = csv.DictWriter(f, write_rows[0].keys())
    writer.writeheader()
    writer.writerows(write_rows)
