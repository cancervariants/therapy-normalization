"""Construct test data for ChEMBL source."""

import contextlib
import sqlite3
from pathlib import Path

from therapy.database import create_db
from therapy.etl import ChEMBL

TEST_IDS = [
    "'CHEMBL11359'",
    "'CHEMBL2068237'",
    "'CHEMBL1628046'",
    "'CHEMBL267014'",
    "'CHEMBL25'",
    "'CHEMBL843'",
    "'CHEMBL40'",
    "'CHEMBL121'",
    "'CHEMBL1006'",
]

ch = ChEMBL(create_db())
ch._extract_data(False)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "chembl"
out_db_path = TEST_DATA_DIR / ch._data_file.name  # type: ignore

with contextlib.suppress(FileNotFoundError):
    out_db_path.unlink()
out_con = sqlite3.connect(out_db_path)
c = out_con.cursor()
c.execute(f"ATTACH DATABASE '{ch._data_file}' AS chembl")

schema_query = """
SELECT sql
FROM chembl.sqlite_master
WHERE
    type='table' AND
    name IN (
        'molecule_dictionary',
        'molecule_synonyms',
        'formulations',
        'drug_indication',
        'products'
    )
"""
schema = [r[0] for r in c.execute(schema_query).fetchall()]
for table_schema in schema:
    c.execute(table_schema)

md_query = f"""
INSERT INTO molecule_dictionary
SELECT *
FROM chembl.molecule_dictionary
WHERE chembl.molecule_dictionary.chembl_id IN (
    {",".join(TEST_IDS)}
)
"""
c.execute(md_query)

ms_query = """
INSERT INTO molecule_synonyms
SELECT cms.* FROM chembl.molecule_synonyms cms
INNER JOIN molecule_dictionary md
ON cms.molregno = md.molregno
"""
c.execute(ms_query)

f_query = """
INSERT INTO formulations
SELECT cf.* FROM chembl.formulations cf
INNER JOIN molecule_dictionary md
ON cf.molregno = md.molregno
"""
c.execute(f_query)

p_query = """
INSERT INTO products
SELECT p.* FROM chembl.products p
INNER JOIN formulations f
ON p.product_id = f.product_id
"""
c.execute(p_query)

d_query = """
INSERT INTO drug_indication
SELECT d.* FROM chembl.drug_indication d
INNER JOIN molecule_dictionary md
ON d.molregno = md.molregno
"""
c.execute(d_query)

out_con.commit()
out_con.close()
