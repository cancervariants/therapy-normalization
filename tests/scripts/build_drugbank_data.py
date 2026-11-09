"""Build DrugBank test data."""

import csv
import logging
from pathlib import Path

from wags_tails import DrugBankData

from therapy.database import create_db
from therapy.etl.drugbank import DrugBank

_logger = logging.getLogger(__name__)

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
try:
    db._extract_data(False)
    SOURCE_DATA_FILE = db._data_file
except Exception as exc:
    SOURCE_DATA_FILE, _ = DrugBankData(
        data_dir=Path.home() / ".local" / "share" / "wags_tails" / "drugbank"
    ).get_latest(from_local=True)
    _logger.warning(
        "DrugBank download failed (%s); using local data: %s", exc, SOURCE_DATA_FILE
    )
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "drugbank"
outfile_path = TEST_DATA_DIR / SOURCE_DATA_FILE.name

with SOURCE_DATA_FILE.open() as f:
    rows = list(csv.DictReader(f))

write_rows = []
for row in rows:
    if row["DrugBank ID"] in TEST_IDS:
        write_rows.append(row)

with outfile_path.open("w") as f:
    writer = csv.DictWriter(f, write_rows[0].keys())
    writer.writeheader()
    writer.writerows(write_rows)
