"""Construct test data for Wikidata"""

import datetime
import json
import logging
from pathlib import Path

from wags_tails.utils.storage import get_data_dir, get_latest_local_file
from wikibaseintegrator.wbi_helpers import execute_sparql_query

from therapy.etl.wikidata import SPARQL_QUERY

_logger = logging.getLogger(__name__)

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

try:
    response = execute_sparql_query(SPARQL_QUERY, max_retries=1, retry_after=0)
except Exception as exc:
    local_file = get_latest_local_file(get_data_dir() / "wikidata", "wikidata_*.json")
    _logger.warning("Wikidata query failed (%s); using local data: %s", exc, local_file)
    with local_file.open() as f:
        records = json.load(f)
else:
    result = response["results"]["bindings"]
    records = [
        {attr: value["value"] for attr, value in item.items()} for item in result
    ]

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
