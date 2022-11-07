"""Build test data for GuideToPharmacology"""
import csv
from pathlib import Path

from therapy.etl import GuideToPHARMACOLOGY
from therapy.database import Database


gtop = GuideToPHARMACOLOGY(Database())
gtop._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
ligands_file_path = TEST_DATA_DIR / gtop._ligands_file.name
mapping_file_path = TEST_DATA_DIR / gtop._mapping_file.name

ligands_rows = []
ligand_ids = {
    "5343",
    "2169",
    "2804",
    "240",
    "3303",
    "5260"
}
with open(gtop._ligands_file, "r") as f:
    reader = csv.reader(f, delimiter="\t")
    ligands_rows.append(next(reader))
    ligands_rows.append(next(reader))

    for row in reader:
        if row[0] in ligand_ids:
            ligands_rows.append(row)

with open(ligands_file_path, "w") as f:
    writer = csv.writer(f)
    writer.writerows(ligands_rows)

map_rows = []
with open(gtop._mapping_file, "r") as f:
    reader = csv.reader(f, delimiter="\t")
    map_rows.append(next(reader))
    map_rows.append(next(reader))

    for row in reader:
        if row[0] in ligand_ids:
            map_rows.append(row)

with open(mapping_file_path, "w") as f:
    writer = csv.writer(f)
    writer.writerows(map_rows)
