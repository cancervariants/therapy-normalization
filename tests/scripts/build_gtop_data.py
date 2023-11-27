"""Build test data for GuideToPharmacology"""
import csv
from pathlib import Path

from therapy.database import Database
from therapy.etl import GuideToPHARMACOLOGY

TEST_IDS = {"5343", "2169", "2804", "240", "3303", "5260"}

gtop = GuideToPHARMACOLOGY(Database())
gtop._extract_data(False)
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "guidetopharmacology"
ligands_file_path = TEST_DATA_DIR / gtop._data_files.ligands.name
mapping_file_path = TEST_DATA_DIR / gtop._data_files.ligand_id_mapping.name

ligands_rows = []
with open(gtop._data_files.ligands, "r") as f:
    reader = csv.reader(f, delimiter="\t")
    ligands_rows.append(next(reader))
    ligands_rows.append(next(reader))

    for row in reader:
        if row[0] in TEST_IDS:
            ligands_rows.append(row)

with open(ligands_file_path, "w") as f:
    writer = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_ALL)
    writer.writerows(ligands_rows)

map_rows = []
with open(gtop._data_files.ligand_id_mapping, "r") as f:
    reader = csv.reader(f, delimiter="\t")
    map_rows.append(next(reader))
    map_rows.append(next(reader))

    for row in reader:
        if row[0] in TEST_IDS:
            map_rows.append(row)

with open(mapping_file_path, "w") as f:
    writer = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_ALL)
    writer.writerows(map_rows)
