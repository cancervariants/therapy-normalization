"""Build drugsatfda test data"""
import json
from pathlib import Path

from therapy.etl import DrugsAtFDA
from therapy.database import Database


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
    "ANDA214475"
]

daf = DrugsAtFDA(Database())
daf._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "drugsatfda"
outfile_path = TEST_DATA_DIR / daf._src_file.name

with open(daf._src_file, "r") as f:
    data = json.load(f)

out_data = {"meta": data["meta"], "results": []}

for record in data["results"]:
    if record["application_number"] in TEST_IDS:
        out_data["results"].append(record)

with open(outfile_path, "w") as f:
    json.dump(out_data, f)
