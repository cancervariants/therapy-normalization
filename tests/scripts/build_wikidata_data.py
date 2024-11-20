"""Construct test data for Wikidata"""

import datetime
import json
from pathlib import Path

from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy.etl.wikidata import SPARQL_QUERY

TEST_IDS = {
    "http://www.wikidata.org/entity/Q412415",
    "http://www.wikidata.org/entity/Q407241",
    "http://www.wikidata.org/entity/Q26272",
    "http://www.wikidata.org/entity/Q15353101",
    "http://www.wikidata.org/entity/Q191924",
    "http://www.wikidata.org/entity/Q418702",
    "http://www.wikidata.org/entity/Q251698",
    "http://www.wikidata.org/entity/Q27287118",
    "http://www.wikidata.org/entity/Q422265",
}

result = execute_sparql_query(SPARQL_QUERY)["results"]["bindings"]
test_data = []
for item in result:
    if item["item"]["value"] in TEST_IDS:
        params = {}
        for attr in item:
            params[attr] = item[attr]["value"]
        test_data.append(params)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "wikidata"
date = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d")
outfile_path = TEST_DATA_DIR / f"wikidata_{date}.json"
with outfile_path.open("w+") as f:
    json.dump(test_data, f, indent=2)
