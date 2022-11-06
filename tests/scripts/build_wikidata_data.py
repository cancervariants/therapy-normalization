"""Construct test data for Wikidata"""
import json
from pathlib import Path
import datetime

from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy.etl.wikidata import SPARQL_QUERY

test_concepts = {
    "http://www.wikidata.org/entity/Q412415",
    "http://www.wikidata.org/entity/Q15353101",
    "http://www.wikidata.org/entity/Q191924",
    "http://www.wikidata.org/entity/Q26272",
    "http://www.wikidata.org/entity/Q418702",
    "http://www.wikidata.org/entity/Q407241",
    "http://www.wikidata.org/entity/Q47521576",
    "http://www.wikidata.org/entity/Q27287118",
    "http://www.wikidata.org/entity/Q422265",
}

result = execute_sparql_query(SPARQL_QUERY)

test_data = []
for item in result["results"]["bindings"]:
    if item["item"]["value"] in test_concepts:
        params = {}
        for attr in item:
            params[attr] = item[attr]["value"]
        test_data.append(params)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
date = datetime.datetime.today().strftime("%Y-%m-%d")
outfile_path = TEST_DATA_DIR / f"wikidata_{date}.json"
with open(outfile_path, "w+") as f:
    json.dump(test_data, f)
