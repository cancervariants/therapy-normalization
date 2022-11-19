"""Build RxNorm test data."""
from pathlib import Path
import csv
import shutil

from therapy.database import Database
from therapy.etl.rxnorm import RxNorm


db = Database()
rx = RxNorm(db)
rx._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "rxnorm"

rxnorm_ids = {
    # unsorted -- return to later
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
    "44",
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
    "2282527",
    "2282518",
    "1041528",
    "891136",
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
}

rows_to_add = []
pins = []
with open(rx._src_file, "r") as f:
    reader = csv.reader(f, delimiter="|")

    for row in reader:
        if row[0] in rxnorm_ids:
            rows_to_add.append(row)

with open(TEST_DATA_DIR / rx._src_file.name, "w") as f:
    writer = csv.writer(f, delimiter="|")
    writer.writerows(rows_to_add)

shutil.copyfile(rx._drug_forms_file, TEST_DATA_DIR / rx._drug_forms_file.name)
