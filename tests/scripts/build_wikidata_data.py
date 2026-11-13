"""Construct test data for Wikidata"""

import datetime
import json
from pathlib import Path

from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy.etl.wikidata import SPARQL_QUERY

TEST_IDS = {
    "http://www.wikidata.org/entity/Q412415",  # Cisplatin: contains normal-rank and deprecated PubChem identifiers.
    "http://www.wikidata.org/entity/Q45044",  # Magnesium citrate: contains preferred- and normal-rank PubChem identifiers.
    "http://www.wikidata.org/entity/Q1041804",  # Imidapril: contains multiple normal-rank PubChem identifiers, with none preferred.
    "http://www.wikidata.org/entity/Q407241",
    "http://www.wikidata.org/entity/Q26272",
    "http://www.wikidata.org/entity/Q15353101",
    "http://www.wikidata.org/entity/Q191924",
    "http://www.wikidata.org/entity/Q418702",
    "http://www.wikidata.org/entity/Q251698",  # Amifostine: contains multiple PubChem identifiers across source rows.
    "http://www.wikidata.org/entity/Q27287118",
    "http://www.wikidata.org/entity/Q422265",  # Spiramycin: contains multiple PubChem identifiers across source rows.
}

values = "VALUES ?item { " + " ".join(f"<{item}>" for item in sorted(TEST_IDS)) + " }"
query = SPARQL_QUERY.replace("WHERE {", "WHERE {\n  " + values, 1)
response = execute_sparql_query(query, max_retries=1, retry_after=0)
result = response["results"]["bindings"]
records = [{attr: value["value"] for attr, value in item.items()} for item in result]

test_data = [item for item in records if item["item"] in TEST_IDS]
missing_ids = TEST_IDS - {item["item"] for item in test_data}
if missing_ids:
    msg = f"Source data is missing required test IDs: {', '.join(sorted(missing_ids))}"
    raise ValueError(msg)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "wikidata"
TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
date = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d")
outfile_path = TEST_DATA_DIR / f"wikidata_{date}.json"
with outfile_path.open("w+") as f:
    json.dump(test_data, f, indent=2)
