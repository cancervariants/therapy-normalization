"""Build RxNorm test data."""

import csv
import shutil
from pathlib import Path

from therapy.database import create_db
from therapy.etl.rxnorm import RXNORM_XREFS, RxNorm

rx = RxNorm(create_db())
rx._extract_data(False)
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "rxnorm"

TEST_IDS = {
    "100213",
    "2555",
    "142424",
    "644",
    "10600",
    "1011",
    "1191",
    "44",
    "61",
    "595",
    "10582",
    "4493",
    "227224",
    "1041527",
    "218330",
    "161",
    "107044",
    "91601",
    "2003328",
    "202856",
    "2618903",
    "2618896",
    "566711",
    "1036822",
    "565073",
    "1036850",
    "977881",
    "12303112",
    "977884",
    "1036819",
    "1036827",
    "826099",
    "725062",
    "799024",
    "575748",
    "647323",
    "563784",
    "576278",
    "575932",
    "2563430",
    "2282527",
    "2282518",
    "565107",
    "565821",
    "569237",
    "565822",
    "861636",
    "573193",
    "565163",
    "542510",
    "491232",
    "670638",
    "564305",
    "563425",
    "2282528",
    "2282519",
    "1041528",
    "891136",
    "8134",
    "9991",
    "4126",
    "1545987",
    "7975",
    # aspirin SBDCs to ensure its trade_names are > 20
    "1247394",
    "572206",
    "576561",
    "572210",
    "573200",
    "568142",
    "571738",
    "994529",
    "572221",
    "1189780",
    "724546",
    "830539",
    "2372236",
    "571342",
    "830531",
    "1925789",
    "828586",
    "260848",
    "647984",
    "1293661",
    # Phenobarbital trade names to ensure > 20
    "1046928",
    "1046809",
    "1046960",
    "1046966",
    "2559715",
    "1047756",
    "1190743",
    "1046971",
    "1047861",
    "1293842",
    "1293742",
    "1046861",
    "1046964",
    "1046816",
    "1046962",
    "1046953",
    "1046876",
    "1728762",
    "1144714",
    "1046848",
    "1048289",
}

rows_to_add = []
pins = []
with rx._data_file.open() as f:
    reader = csv.reader(f, delimiter="|")

    for row in reader:
        if row[0] in TEST_IDS and row[11] in RXNORM_XREFS:
            rows_to_add.append(row)

with (TEST_DATA_DIR / rx._data_file.name).open("w") as f:
    writer = csv.writer(f, delimiter="|")
    writer.writerows(rows_to_add)

shutil.copyfile(rx._drug_forms_file, TEST_DATA_DIR / rx._drug_forms_file.name)
