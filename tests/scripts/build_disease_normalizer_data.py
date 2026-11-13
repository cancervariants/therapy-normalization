"""Construct static mapping of disease normalizer input.
Assumes complete and functioning disease normalizer endpoint is available.
"""

import json
from pathlib import Path

from disease.database import create_db as create_disease_db
from disease.query import QueryHandler as DiseaseQueryHandler
from disease.schemas import NormalizationService as DiseaseNormalizationService

from therapy.database.dynamodb import DynamoDatabase
from therapy.etl import ChEMBL, HemOnc

TEST_ROOT = Path(__file__).resolve().parents[1]
TEST_DATA_DIRECTORY = TEST_ROOT / "data"


class ReadOnlyDatabase(DynamoDatabase):
    """Provide read-only instance of database for security's sake"""

    def add_record(self, record: dict, record_type: str = "identity") -> None:
        """Add new record to database"""


db = ReadOnlyDatabase()
disease_normalizer_table = {}


class SaveQueryHandler(DiseaseQueryHandler):
    """Disease query class which saves lookups and results"""

    def _add_merged_meta(self, response: dict) -> dict:
        """Skip source metadata because fixtures only store disease IDs.

        :param response: normalization response being assembled
        :return: response without source metadata lookups
        """
        return response

    def normalize(self, query: str) -> DiseaseNormalizationService:
        """Normalize a query and save its disease ID for the fixture.

        :param query: disease term or identifier
        :return: normalization response
        """
        response = super().normalize(query)
        result = (
            response.disease.id.removeprefix("normalize.disease.")
            if response.disease
            else None
        )
        disease_normalizer_table[query.lower()] = result
        return response


disease_query_handler = SaveQueryHandler(create_disease_db())

ch = ChEMBL(database=db, data_path=TEST_DATA_DIRECTORY / "chembl")
ch.disease_normalizer = disease_query_handler
ch.perform_etl(use_existing=True)

h = HemOnc(database=db, data_path=TEST_DATA_DIRECTORY / "hemonc")
h.disease_normalizer = disease_query_handler
h.perform_etl(use_existing=True)


with (TEST_DATA_DIRECTORY / "disease_normalization.json").open("w") as f:
    # for consistency/easier diffing
    sorted_dict = {  # noqa: C416
        key: value for key, value in sorted(disease_normalizer_table.items())
    }
    json.dump(sorted_dict, f, indent=2)
