"""Build drugsatfda test data"""

import json
from pathlib import Path

from therapy.database import create_db
from therapy.etl import DrugsAtFDA

TEST_IDS = [
    "NDA020221",
    "NDA022334",
    "NDA050682",
    "ANDA074656",
    "ANDA075036",
    "ANDA074735",
    "ANDA206774",
    "ANDA207323",
    "NDA018057",
    "ANDA072267",
    "NDA017604",
    "NDA210595",
    "NDA202450",
    "NDA091141",
    "NDA022007",
    "NDA050682",
    "NDA017604",
    "ANDA214475",
]

daf = DrugsAtFDA(create_db())
daf._extract_data(False)
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "drugsatfda"
outfile_path = TEST_DATA_DIR / daf._data_file.name

with daf._data_file.open() as f:
    data = json.load(f)

out_data = {"meta": data["meta"], "results": []}

for record in data["results"]:
    if record["application_number"] in TEST_IDS:
        out_data["results"].append(record)

with outfile_path.open("w") as f:
    json.dump(out_data, f)
